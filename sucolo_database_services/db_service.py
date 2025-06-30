import logging
from typing import Any, Literal

import geopandas as gpd
import pandas as pd
from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field, ValidationError, field_validator
from redis import Redis

from sucolo_database_services.elasticsearch_client.index_manager import (
    default_mapping,
)
from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService,
)
from sucolo_database_services.redis_client.consts import POIS_SUFFIX
from sucolo_database_services.redis_client.service import RedisService
from sucolo_database_services.utils.config import Config
from sucolo_database_services.utils.exceptions import CityNotFoundError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

HEX_ID_TYPE = str


class Query(BaseModel):
    """Base class for all queries."""

    city: str = Field(..., description="City name to query data for")
    resolution: int = Field(..., description="Hexagon resolution level")

    @field_validator("city")
    def validate_city(cls, city: str) -> str:
        if not city:
            raise ValidationError("City name cannot be empty")
        return city

    @field_validator("resolution")
    def validate_resolution(cls, resolution: int) -> int:
        if resolution <= 0:
            raise ValidationError("Resolution must be a positive integer")
        return resolution


class AmenityFields(BaseModel):
    """Dynamic features query fields for a specific amenity type."""

    amenity: str
    radius: int = Field(gt=0, description="Radius must be positive")
    penalty: int | None = Field(
        default=None, ge=0, description="Penalty must be non-negative"
    )

    @field_validator("radius")
    def validate_radius(cls, radius: int) -> int:
        if radius <= 0:
            raise ValidationError("Radius must be positive")
        return radius


class AmenityQuery(Query, AmenityFields):
    pass


class StaticFeatureFields(BaseModel):
    """Static features query fields for hexagons.
    These values are comming from districts and are the same for all
    hexagons falling into the same district."""

    features: list[str]


class StaticFeatureQuery(Query, StaticFeatureFields):
    pass


class DataQuery(Query):
    nearests: list[AmenityFields] = []
    counts: list[AmenityFields] = []
    presences: list[AmenityFields] = []
    hexagons: StaticFeatureFields | None = None

    def __post_model_init__(self) -> None:
        def check(
            l_q: list[AmenityFields] | StaticFeatureFields | None,
        ) -> bool:
            return l_q is None or (
                not isinstance(l_q, StaticFeatureFields) and len(l_q) == 0
            )

        if (
            check(self.nearests)
            and check(self.counts)
            and check(self.presences)
            and check(self.hexagons)
        ):
            raise ValueError(
                "At least one of the queries type has to be defined."
            )


def fields_to_queries(
    query: DataQuery,
    type_: Literal["nearests", "counts", "presences"],
) -> list[AmenityQuery]:
    """Convert a list of AmenityFields to a list of AmenityQuery."""
    return [
        AmenityQuery(
            city=query.city,
            resolution=query.resolution,
            amenity=fields.amenity,
            radius=fields.radius,
            penalty=fields.penalty,
        )
        for fields in getattr(query, type_)
    ]


class DBService:
    """Service for managing database operations across Elasticsearch and Redis.

    This service provides methods for querying and managing geographical data,
    including POIs (Points of Interest), districts, and hexagons.
    """

    def __init__(
        self,
        config: Config,
    ) -> None:
        """Initialize the database service with configuration.

        Args:
            config: Configuration object containing all necessary settings
        """
        assert (
            config.database.ca_certs.is_file()
        ), f"File {config.database.ca_certs} not found."
        self.es_service = ElasticsearchService(
            Elasticsearch(
                hosts=[config.database.elastic_host],
                basic_auth=(
                    config.database.elastic_user,
                    config.database.elastic_password,
                ),
                ca_certs=str(config.database.ca_certs),
                timeout=config.database.elastic_timeout,
            )
        )
        self.redis_service = RedisService(
            Redis(
                host=config.database.redis_host,
                port=config.database.redis_port,
                db=config.database.redis_db,
            )
        )

    def get_cities(self) -> list[str]:
        """Get list of all available cities."""

        cities = self.es_service.get_all_indices()
        cities = list(filter(lambda city: city[0] != ".", cities))
        return cities

    def city_data_exists(self, city: str) -> bool:
        """Check if city data exists in Elasticsearch."""
        return self.es_service.index_manager.index_exists(city)

    def get_amenities(self, city: str) -> list[str]:
        """Get list of all amenities for a given city."""

        city_keys = self.redis_service.keys_manager.get_city_keys(city)
        poi_keys = list(
            filter(
                lambda key: key[-len(POIS_SUFFIX) :] == POIS_SUFFIX,
                city_keys,
            )
        )
        amenities = list(
            map(
                lambda key: key[len(city) + 1 : -len(POIS_SUFFIX)],
                poi_keys,
            )
        )
        return amenities

    def get_district_attributes(self, city: str) -> list[str]:
        """Get list of all district attributes for a given city."""

        district_data = self.es_service.read.get_districts(
            index_name=city,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(columns=["district", "polygon", "type"], errors="ignore")
        df = df.dropna()  # get features only available for all districts
        district_attributes = list(df.columns)
        return district_attributes

    def get_multiple_features(self, query: DataQuery) -> pd.DataFrame:
        """Get multiple features for a given city based on the query parameters.

        This method combines different types of features (nearest distances,
        counts, presences, and hexagon features) into a single DataFrame.

        Args:
            query: DataQuery object containing the query parameters

        Returns:
            DataFrame containing all requested features indexed by hex_id
        """
        # Validate city exists
        if query.city not in self.get_cities():
            raise CityNotFoundError(f"City {query.city} not found")

        index = pd.Index(
            self.es_service.read.get_hexagons(
                index_name=query.city,
                resolution=query.resolution,
                features=[],
                only_location=True,
            ).keys()
        )
        df = pd.DataFrame(index=index)

        # Process nearest distances
        for subquery in fields_to_queries(query, "nearests"):
            nearest_feature = self.calculate_nearest_distances(query=subquery)
            df = df.join(
                pd.Series(
                    nearest_feature,
                    name="nearest_" + subquery.amenity,
                )
            )

        # Process counts
        for subquery in fields_to_queries(query, "counts"):
            count_feature = self.count_pois_in_distance(
                query=subquery,
            )
            df = df.join(
                pd.Series(
                    count_feature,
                    name="count_" + subquery.amenity,
                )
            )

        # Process presences
        for subquery in fields_to_queries(query, "presences"):
            presence_feature = self.determine_presence_in_distance(
                query=subquery,
            )
            df = df.join(
                pd.Series(
                    presence_feature,
                    name="present_" + subquery.amenity,
                )
            )

        # Process hexagon features
        if query.hexagons is not None:
            hexagon_features = self.get_hexagon_static_features(
                city=query.city,
                resolution=query.resolution,
                feature_columns=query.hexagons.features,
            )
            df = df.join(hexagon_features)

        return df

    def calculate_nearest_distances(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, float | None]:
        """Calculate nearest distances for a given amenity type.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to nearest distance or None
        """
        nearest_distances = (
            self.redis_service.read.find_nearest_pois_to_hex_centers(
                city=query.city,
                amenity=query.amenity,
                resolution=query.resolution,
                radius=query.radius,
                count=1,
            )
        )
        first_nearest_distances = self._nearest_post_processing(
            nearest_distances=nearest_distances,
            radius=query.radius,
            penalty=query.penalty,
        )
        return first_nearest_distances

    def _nearest_post_processing(
        self,
        nearest_distances: dict[str, list[float]],
        radius: int,
        penalty: int | None,
    ) -> dict[str, float | None]:
        """Post-process nearest distances with optional penalty.

        Args:
            nearest_distances: Dictionary of hex_id to list of distances
            radius: Search radius
            penalty: Optional penalty to add when no POI is found

        Returns:
            Dictionary mapping hex_id to processed distance
        """
        if penalty is None:
            first_nearest_distances = {
                hex_id: (dists[0] if len(dists) > 0 else None)
                for hex_id, dists in nearest_distances.items()
            }
        else:
            first_nearest_distances = {
                hex_id: (dists[0] if len(dists) > 0 else radius + penalty)
                for hex_id, dists in nearest_distances.items()
            }
        return first_nearest_distances

    def count_pois_in_distance(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, int]:
        """Count POIs within a given radius.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to count of POIs
        """
        nearest_pois = self.redis_service.read.find_nearest_pois_to_hex_centers(
            city=query.city,
            amenity=query.amenity,
            resolution=query.resolution,
            radius=query.radius,
            count=None,
        )
        counts = {hex_id: len(pois) for hex_id, pois in nearest_pois.items()}
        return counts

    def determine_presence_in_distance(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, int]:
        """Determine if any POIs are present within a given radius.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to presence indicator
            (1 if present, 0 if not)
        """
        nearest_pois = self.redis_service.read.find_nearest_pois_to_hex_centers(
            city=query.city,
            amenity=query.amenity,
            resolution=query.resolution,
            radius=query.radius,
            count=None,
        )
        presence = {
            hex_id: (1 if len(pois) > 0 else 0)
            for hex_id, pois in nearest_pois.items()
        }
        return presence

    def get_hexagon_static_features(
        self,
        city: str,
        feature_columns: list[str],
        resolution: int,
    ) -> pd.DataFrame:
        """Get static features for hexagons.

        Args:
            city: City name
            feature_columns: List of feature columns to retrieve

        Returns:
            DataFrame containing the requested features
        """
        district_data = self.es_service.read.get_hexagons(
            index_name=city,
            features=feature_columns,
            resolution=resolution,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(
            columns=[
                col
                for col in df.columns
                if col in ["hex_id", "type", "location", "polygon"]
            ]
        )
        return df

    def delete_city_data(
        self,
        city: str,
        ignore_if_index_not_exist: bool = True,
    ) -> None:
        """Delete all data for a given city from both Elasticsearch and Redis.

        Args:
            city: City name to delete data for
            ignore_if_index_not_exist: Whether to ignore if the index
                doesn't exist
        """
        try:
            self.es_service.index_manager.delete_index(
                index_name=city,
                ignore_if_index_not_exist=ignore_if_index_not_exist,
            )
            logger.info(f'Elasticsearch data for city "{city}" deleted.')

            self.redis_service.keys_manager.delete_city_keys(city)
            logger.info(f'Redis data for city "{city}" deleted.')
        except Exception as e:
            logger.error(f"Error deleting city data for {city}: " f"{str(e)}")
            raise

    def upload_new_pois(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
    ) -> None:
        """Upload new POIs to the database.

        Args:
            city: City name
            pois_gdf: GeoDataFrame containing the POIs to upload
        """
        self.es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
        logger.info("PoIs uploaded to elasticsearch.")
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city, pois=pois_gdf
        )
        logger.info("PoIs uploaded to redis.")

    def upload_city_data(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: int | list[int] = 9,
        ignore_if_index_exists: bool = True,
        es_index_mapping: dict[str, Any] = default_mapping,
    ) -> None:
        """Upload complete city data including POIs, districts, and hexagons."""
        if isinstance(hex_resolutions, int):
            hex_resolutions = [hex_resolutions]
        try:
            logger.info(f'UPLOADING DATA FOR CITY "{city}" ...')
            self._upload_city_data_elasticsearch(
                city=city,
                pois_gdf=pois_gdf,
                district_gdf=district_gdf,
                hex_resolutions=hex_resolutions,
                ignore_if_index_exists=ignore_if_index_exists,
                es_index_mapping=es_index_mapping,
            )
            self._upload_city_data_redis(
                city=city,
                pois_gdf=pois_gdf,
                district_gdf=district_gdf,
                hex_resolutions=hex_resolutions,
            )
            logger.info(f"Successfully uploaded all data for city {city}.")
        except Exception as e:
            logger.error(f"Error uploading city data for {city}: " f"{str(e)}")
            raise

    def _upload_city_data_elasticsearch(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: list[int],
        ignore_if_index_exists: bool,
        es_index_mapping: dict[str, Any],
    ) -> None:
        """Upload city data to Elasticsearch
        (index, POIs, districts, hexagons)."""
        logger.info(f'Creating index "{city}" in elasticsearch.')
        self.es_service.index_manager.create_index(
            index_name=city,
            ignore_if_exists=ignore_if_index_exists,
            mapping=es_index_mapping,
        )
        logger.info(f'Index "{city}" created in elasticsearch.')

        logger.info("Uploading POIs to elasticsearch.")
        self.es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
        logger.info("PoIs uploaded to elasticsearch.")
        logger.info("Uploading districts to elasticsearch.")
        self.es_service.write.upload_districts(
            index_name=city, gdf=district_gdf
        )
        logger.info("Districts uploaded to elasticsearch.")
        for hex_resolution in hex_resolutions:
            logger.info(
                "Uploading hexagons to elasticsearch "
                f"with resolution {hex_resolution}."
            )
            self.es_service.write.upload_hex_centers(
                index_name=city,
                districts=district_gdf,
                hex_resolution=hex_resolution,
            )
        logger.info("Hexagons uploaded to elasticsearch.")

    def _upload_city_data_redis(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: list[int],
    ) -> None:
        """Upload city data to Redis (POIs, wheelchair POIs, hexagons)."""
        logger.info(f'Creating keys for city "{city}" in redis.')
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city, pois=pois_gdf
        )
        logger.info("PoIs uploaded to redis.")
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city,
            pois=pois_gdf,
            only_wheelchair_accessible=True,
            wheelchair_positive_values=["yes"],
        )
        logger.info("Wheelchair accessible PoIs uploaded to redis.")
        for hex_resolution in hex_resolutions:
            logger.info(
                "Uploading hexagons to redis "
                f"with resolution {hex_resolution}."
            )
            self.redis_service.write.upload_hex_centers(
                city=city, districts=district_gdf, resolution=hex_resolution
            )
        logger.info("Hexagons uploaded to redis.")

    def count_records_per_amenity(self, city: str) -> dict[str, int]:
        result = self.redis_service.read.count_records_per_key(city)
        return result

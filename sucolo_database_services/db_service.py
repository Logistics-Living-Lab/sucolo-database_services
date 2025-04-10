import logging
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field, field_validator
from redis import Redis

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService,
)
from sucolo_database_services.redis_client.consts import POIS_SUFFIX
from sucolo_database_services.redis_client.service import RedisService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

HEX_ID_TYPE = str


class AmenityQuery(BaseModel):
    amenity: str
    radius: int = Field(gt=0, description="Radius must be positive")
    penalty: Optional[int] = Field(
        default=None, ge=0, description="Penalty must be non-negative"
    )

    @field_validator("radius")
    def validate_radius(cls, radius: int) -> int:
        if radius <= 0:
            raise ValueError("Radius must be positive")
        return radius


class HexagonQuery(BaseModel):
    features: list[str]


class DataQuery(BaseModel):
    city: str
    nearests: list[AmenityQuery] = []
    counts: list[AmenityQuery] = []
    presences: list[AmenityQuery] = []
    hexagons: HexagonQuery | None = None

    def __post_model_init__(self) -> None:
        def check(l_q: list[AmenityQuery] | HexagonQuery | None) -> bool:
            return l_q is None or (
                not isinstance(l_q, HexagonQuery) and len(l_q) == 0
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


class DBService:
    """Service for managing database operations across Elasticsearch and Redis.

    This service provides methods for querying and managing geographical data,
    including POIs (Points of Interest), districts, and hexagons.
    """

    def __init__(
        self,
        elastic_host: str,
        elastic_user: str,
        elastic_password: str,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        ca_certs: Path = Path("certs/ca.crt"),
    ) -> None:
        """Initialize the database service with Elasticsearch and Redis
        connections.

        Args:
            elastic_host: Elasticsearch host URL
            elastic_user: Elasticsearch username
            elastic_password: Elasticsearch password
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            ca_certs: Path to CA certificates file
        """
        assert ca_certs.is_file(), f"File {ca_certs} not found."
        self.es_service = ElasticsearchService(
            Elasticsearch(
                hosts=[elastic_host],
                basic_auth=(elastic_user, elastic_password),
                ca_certs=str(ca_certs),
            )
        )
        self.redis_service = RedisService(
            Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
            )
        )

    def get_cities(self) -> list[str]:
        cities = self.es_service.get_all_indices()
        cities = list(filter(lambda city: city[0] != ".", cities))
        return cities

    def get_amenities(self, city: str) -> list[str]:
        city_keys = self.redis_service.keys_manager.get_city_keys(city)
        poi_keys = list(
            filter(
                lambda key: key[-len(POIS_SUFFIX) :] == POIS_SUFFIX, city_keys
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
        district_data = self.es_service.read.get_districts(
            index_name=city,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(columns=["district", "polygon", "type"])
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
        index = pd.Index(
            self.es_service.read.get_hexagons(
                index_name=query.city, features=[], only_location=True
            ).keys()
        )
        df = pd.DataFrame(index=index)

        # Process nearest distances
        for subquery in query.nearests:
            nearest_feature = self.calculate_nearests_distances(
                city=query.city,
                amenity=subquery.amenity,
                radius=subquery.radius,
                penalty=subquery.penalty,
            )
            df = df.join(
                pd.Series(nearest_feature, name="nearest_" + subquery.amenity)
            )

        for subquery in query.counts:
            count_feature = self.count_pois_in_distance(
                city=query.city,
                amenity=subquery.amenity,
                radius=subquery.radius,
            )
            df = df.join(
                pd.Series(count_feature, name="count_" + subquery.amenity)
            )
        # Process presences
        for subquery in query.presences:
            presence_feature = self.determine_presence_in_distance(
                city=query.city,
                amenity=subquery.amenity,
                radius=subquery.radius,
            )
            df = df.join(
                pd.Series(presence_feature, name="present_" + subquery.amenity)
            )
        # Process hexagon features
        if query.hexagons is not None:
            hexagon_features = self.get_hexagon_static_features(
                city=query.city, feature_columns=query.hexagons.features
            )
            df = df.join(hexagon_features)

        return df

    def calculate_nearests_distances(
        self,
        city: str,
        amenity: str,
        radius: int,
        penalty: int | None,
    ) -> dict[HEX_ID_TYPE, float | None]:
        nearest_distances = self.redis_service.read.nearest_pois_to_hex_centers(
            city=city,
            amenity=amenity,
            radius=radius,
            unit="m",
            count=1,
        )
        first_nearest_distances = self._nearest_post_processing(
            nearest_distances=nearest_distances,
            radius=radius,
            penalty=penalty,
        )
        return first_nearest_distances

    def _nearest_post_processing(
        self,
        nearest_distances: dict[str, list[float]],
        radius: int,
        penalty: int | None,
    ) -> dict[str, float | None]:
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
        self, city: str, amenity: str, radius: int
    ) -> dict[HEX_ID_TYPE, int]:
        nearest_pois = self.redis_service.read.nearest_pois_to_hex_centers(
            city=city,
            amenity=amenity,
            radius=radius,
            unit="m",
            count=None,
        )
        counts = {hex_id: len(pois) for hex_id, pois in nearest_pois.items()}
        return counts

    def determine_presence_in_distance(
        self, city: str, amenity: str, radius: int
    ) -> dict[HEX_ID_TYPE, int]:
        """Determine if any POIs of a given amenity type are present within
        the specified radius.

        Args:
            city: City name
            amenity: Type of amenity to check
            radius: Search radius in meters

        Returns:
            Dictionary mapping hex_id to presence indicator
            (1 if present, 0 if not)
        """
        nearest_pois = self.redis_service.read.nearest_pois_to_hex_centers(
            city=city,
            amenity=amenity,
            radius=radius,
            unit="m",
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
    ) -> pd.DataFrame:
        district_data = self.es_service.read.get_hexagons(
            index_name=city,
            features=feature_columns,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(
            columns=[
                col
                for col in df.columns
                if col in ["hex_id", "type", "location"]
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
        distric_gdf: gpd.GeoDataFrame,
        hex_resolution: int = 9,
        ignore_if_index_exists: bool = True,
    ) -> None:
        """Upload complete city data including POIs, districts, and hexagons.

        Args:
            city: City name
            pois_gdf: GeoDataFrame containing POIs
            distric_gdf: GeoDataFrame containing districts
            hex_resolution: Resolution for hexagon grid
        """
        try:
            self.es_service.index_manager.create_index(
                index_name=city,
                ignore_if_exists=ignore_if_index_exists,
            )
            logger.info(f'Index "{city}" created in elasticsearch.')

            self.es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
            logger.info(f"PoIs uploaded to elasticsearch for index {city}.")

            self.es_service.write.upload_districts(
                index_name=city, gdf=distric_gdf
            )
            logger.info(
                f"Districts uploaded to elasticsearch for index {city}."
            )

            # Uploading hexagon centers
            self.es_service.write.upload_hex_centers(
                index_name=city,
                districts=distric_gdf,
                hex_resolution=hex_resolution,
            )
            logger.info(f"Hexagons uploaded to elasticsearch for index {city}.")

            self.redis_service.write.upload_hex_centers(
                city=city, districts=distric_gdf, resolution=hex_resolution
            )
            logger.info(f"Hexagons uploaded to redis for city {city}.")
            self.redis_service.write.upload_pois_by_amenity_key(
                city=city, pois=pois_gdf
            )
            logger.info(f"PoIs uploaded to redis for city {city}.")
            self.redis_service.write.upload_pois_by_amenity_key(
                city=city,
                pois=pois_gdf,
                only_wheelchair_accessible=True,
                wheelchair_positive_values=["yes"],
            )
            logger.info(
                f"Wheelchair accessible PoIs uploaded to redis for city {city}."
            )
            logger.info(f"Successfully uploaded all data for city {city}")
        except Exception as e:
            logger.error(f"Error uploading city data for {city}: " f"{str(e)}")
            raise

    def count_records_per_amenity(self, city: str) -> dict[str, int]:
        result = self.redis_service.read.count_records_per_key(city)
        return result

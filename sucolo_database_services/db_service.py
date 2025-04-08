from pathlib import Path

import geopandas as gpd
import pandas as pd
from elasticsearch import Elasticsearch
from pydantic import BaseModel
from redis import Redis

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService,
)
from sucolo_database_services.redis_client.consts import POIS_SUFFIX
from sucolo_database_services.redis_client.service import RedisService

HEX_ID_TYPE = str


class AmenityQuery(BaseModel):
    amenity: str
    radius: int
    penalty: int | None = None


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
        index = pd.Index(
            self.es_service.read.get_hexagons(
                index_name=query.city, features=[], only_location=True
            ).keys()
        )
        df = pd.DataFrame(index=index)
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
        for subquery in query.presences:
            presence_feature = self.determin_presence_in_distance(
                city=query.city,
                amenity=subquery.amenity,
                radius=subquery.radius,
            )
            df = df.join(
                pd.Series(presence_feature, name="present_" + subquery.amenity)
            )
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

    def determin_presence_in_distance(
        self, city: str, amenity: str, radius: int
    ) -> dict[HEX_ID_TYPE, int]:
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
        self.es_service.index_manager.delete_index(
            index_name=city, ignore_if_index_not_exist=ignore_if_index_not_exist
        )
        print(f'Elasticsearch data for city "{city}" deleted.')
        self.redis_service.keys_manager.delete_city_keys(city)
        print(f'Redi data for city "{city}" deleted.')

    def upload_new_pois(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
    ) -> None:
        self.es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
        print("PoIs uploaded to elasticsearch.")
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city, pois=pois_gdf
        )
        print("PoIs uploaded to redis.")

    def upload_city_data(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        distric_gdf: gpd.GeoDataFrame,
        hex_resolution: int = 9,
        ignore_if_index_exists: bool = True,
    ) -> None:
        self.es_service.index_manager.create_index(
            index_name=city,
            ignore_if_exists=ignore_if_index_exists,
        )
        print("Index created.")

        self.es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
        print("PoIs uploaded.")

        self.es_service.write.upload_districts(index_name=city, gdf=distric_gdf)
        print("Districts uploaded.")

        # Uploading hexagon centers
        self.es_service.write.upload_hex_centers(
            index_name=city,
            districts=distric_gdf,
            hex_resolution=hex_resolution,
        )
        print("Hexagons uploaded.")

        print("REDIS PART")
        self.redis_service.write.upload_hex_centers(
            city=city, districts=distric_gdf, resolution=hex_resolution
        )
        print("Hexagons uploaded.")
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city, pois=pois_gdf
        )
        print("PoIs uploaded.")
        self.redis_service.write.upload_pois_by_amenity_key(
            city=city,
            pois=pois_gdf,
            only_wheelchair_accessible=True,
            wheelchair_positive_values=["yes"],
        )
        print("Wheelchair accessible PoIs uploaded.")

    def count_records_per_amenity(self, city: str) -> dict[str, int]:
        result = self.redis_service.read.count_records_per_key(city)
        return result

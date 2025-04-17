from typing import Any

import geopandas as gpd
import pandas as pd
from _typeshed import Incomplete
from pydantic import BaseModel, Field

from sucolo_database_services.elasticsearch_client.index_manager import (
    default_mapping as default_mapping,
)
from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService as ElasticsearchService,
)
from sucolo_database_services.redis_client.consts import (
    POIS_SUFFIX as POIS_SUFFIX,
)
from sucolo_database_services.redis_client.service import (
    RedisService as RedisService,
)
from sucolo_database_services.utils.config import Config as Config
from sucolo_database_services.utils.exceptions import (
    CityNotFoundError as CityNotFoundError,
)

logger: Incomplete
HEX_ID_TYPE = str

class AmenityQuery(BaseModel):
    amenity: str
    radius: int = Field(gt=0, description="Radius must be positive")
    penalty: int | None = Field(
        default=None, ge=0, description="Penalty must be non-negative"
    )
    def validate_radius(cls, radius: int) -> int: ...

class HexagonQuery(BaseModel):
    features: list[str]

class DataQuery(BaseModel):
    city: str
    nearests: list[AmenityQuery]
    counts: list[AmenityQuery]
    presences: list[AmenityQuery]
    hexagons: HexagonQuery | None
    def __post_model_init__(self) -> None: ...

class DBService:
    es_service: Incomplete
    redis_service: Incomplete
    def __init__(self, config: Config) -> None: ...
    def get_cities(self) -> list[str]: ...
    def city_data_exists(self, city: str) -> bool: ...
    def get_amenities(self, city: str) -> list[str]: ...
    def get_district_attributes(self, city: str) -> list[str]: ...
    def get_multiple_features(self, query: DataQuery) -> pd.DataFrame: ...
    def calculate_nearest_distances(
        self, city: str, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, float | None]: ...
    def count_pois_in_distance(
        self, city: str, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, int]: ...
    def determine_presence_in_distance(
        self, city: str, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, int]: ...
    def get_hexagon_static_features(
        self, city: str, feature_columns: list[str]
    ) -> pd.DataFrame: ...
    def delete_city_data(
        self, city: str, ignore_if_index_not_exist: bool = True
    ) -> None: ...
    def upload_new_pois(
        self, city: str, pois_gdf: gpd.GeoDataFrame
    ) -> None: ...
    def upload_city_data(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolution: int = 9,
        ignore_if_index_exists: bool = True,
        es_index_mapping: dict[str, Any] = ...,
    ) -> None: ...
    def count_records_per_amenity(self, city: str) -> dict[str, int]: ...

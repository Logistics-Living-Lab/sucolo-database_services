from pathlib import Path
from typing import Any

import geopandas as gpd

from sucolo_database_services.elasticsearch_client.index_manager import (
    default_mapping as default_mapping,
)
from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)

class _Upload(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def upload_city_data(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: int | list[int] = 9,
        ignore_if_index_exists: bool = True,
        es_index_mapping: dict[str, Any] = ...,
    ) -> None: ...
    def upload_city_data_from_files(
        self, city: str, hex_resolutions: int | list[int], data_dir: Path = ...
    ) -> None: ...

class _Delete(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def delete_city_data(
        self, city: str, ignore_if_index_not_exist: bool = True
    ) -> None: ...

class DataManagementService(_Upload, _Delete):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...

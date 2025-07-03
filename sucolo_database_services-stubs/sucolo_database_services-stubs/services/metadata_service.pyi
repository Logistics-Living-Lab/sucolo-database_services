from sucolo_database_services.redis_client.consts import (
    POIS_SUFFIX as POIS_SUFFIX,
)
from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)

class MetadataService(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def get_cities(self) -> list[str]: ...
    def city_data_exists(self, city: str) -> bool: ...
    def get_amenities(self, city: str) -> list[str]: ...
    def get_district_attributes(self, city: str) -> list[str]: ...

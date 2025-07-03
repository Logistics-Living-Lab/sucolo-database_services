from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)
from sucolo_database_services.services.fields_and_queries import (
    AmenityQuery as AmenityQuery,
)

HEX_ID_TYPE = str

class DynamicFeaturesService(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def calculate_nearest_distances(
        self, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, float | None]: ...
    def count_pois_in_distance(
        self, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, int]: ...
    def determine_presence_in_distance(
        self, query: AmenityQuery
    ) -> dict[HEX_ID_TYPE, int]: ...

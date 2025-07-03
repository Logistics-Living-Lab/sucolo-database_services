from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)

class HealthCheckService(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def check_elasticsearch(self) -> bool: ...
    def check_redis(self) -> bool: ...

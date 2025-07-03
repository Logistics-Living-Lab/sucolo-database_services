from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)


class HealthCheckService(BaseService):
    """Service for health checks of the database services."""

    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super(HealthCheckService, self).__init__(base_service_dependencies)

    def check_elasticsearch(self) -> bool:
        """Check if Elasticsearch is reachable."""
        try:
            return self.es_service.check_health()
        except Exception as e:
            self.logger.error(f"Elasticsearch health check failed: {e}")
            return False

    def check_redis(self) -> bool:
        """Check if Redis is reachable."""
        try:
            return self.redis_service.check_health()
        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")
            return False

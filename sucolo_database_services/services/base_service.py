import abc
from dataclasses import dataclass
from logging import Logger

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService,
)
from sucolo_database_services.redis_client.service import RedisService


@dataclass
class BaseServiceDependencies:
    es_service: ElasticsearchService
    redis_service: RedisService
    logger: Logger

    def __post_init__(self) -> None:
        if not isinstance(self.es_service, ElasticsearchService):
            raise TypeError(
                "es_service must be an instance of ElasticsearchService"
            )
        if not isinstance(self.redis_service, RedisService):
            raise TypeError("redis_service must be an instance of RedisService")
        if not isinstance(self.logger, Logger):
            raise TypeError("logger must be an instance of Logger")


class BaseService(abc.ABC):
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        self._es_service = base_service_dependencies.es_service
        self._redis_service = base_service_dependencies.redis_service
        self._logger = base_service_dependencies.logger

import abc
from dataclasses import dataclass
from logging import Logger

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService as ElasticsearchService,
)
from sucolo_database_services.redis_client.service import (
    RedisService as RedisService,
)

@dataclass
class BaseServiceDependencies:
    es_service: ElasticsearchService
    redis_service: RedisService
    logger: Logger
    def __post_init__(self) -> None: ...

class BaseService(abc.ABC):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...

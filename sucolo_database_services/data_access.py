import logging

from elasticsearch import Elasticsearch
from redis import Redis

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService,
)
from sucolo_database_services.redis_client.service import RedisService
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies,
)
from sucolo_database_services.services.data_management_service import (
    DataManagementService,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService,
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService,
)
from sucolo_database_services.services.health_check_service import (
    HealthCheckService,
)
from sucolo_database_services.services.metadata_service import MetadataService
from sucolo_database_services.services.multiple_features_service import (
    MultipleFeaturesService,
)
from sucolo_database_services.utils.config import Config, LoggingConfig


class DataAccess:
    """Service for managing database operations across Elasticsearch and Redis.

    This service provides methods for querying and managing geographical data,
    including POIs (Points of Interest), districts, and hexagons.
    """

    def __init__(
        self,
        config: Config,
    ) -> None:
        """Initialize the database service with configuration.

        Args:
            config: Configuration object containing all necessary settings
        """
        assert (
            config.database.ca_certs.is_file()
        ), f"File {config.database.ca_certs} not found."

        self.logger = self._get_logger(config.logging)
        self._es_service = ElasticsearchService(
            Elasticsearch(
                hosts=[config.database.elastic_host],
                basic_auth=(
                    config.database.elastic_user,
                    config.database.elastic_password,
                ),
                ca_certs=str(config.database.ca_certs),
                timeout=config.database.elastic_timeout,
            )
        )
        self._redis_service = RedisService(
            Redis(
                host=config.database.redis_host,
                port=config.database.redis_port,
                db=config.database.redis_db,
            )
        )

        base_service_dependencies = BaseServiceDependencies(
            es_service=self._es_service,
            redis_service=self._redis_service,
            logger=self.logger,
        )
        self.dynamic_features = DynamicFeaturesService(
            base_service_dependencies
        )
        self.district_features = DistrictFeaturesService(
            base_service_dependencies
        )
        self.data_management = DataManagementService(base_service_dependencies)
        self.metadata = MetadataService(base_service_dependencies)
        self.health_check = HealthCheckService(base_service_dependencies)

        self.multiple_features = MultipleFeaturesService(
            base_service_dependencies=base_service_dependencies,
            metadata_service=self.metadata,
            dynamic_features_service=self.dynamic_features,
            district_features_service=self.district_features,
        )

    def _get_logger(self, logging_config: LoggingConfig) -> logging.Logger:
        """Set the logger configuration."""
        logger = logging.getLogger("sucolo_database_services")
        logger.setLevel(logging_config.level)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(logging_config.format))
        logger.addHandler(handler)

        if logging_config.file:
            file_handler = logging.FileHandler(logging_config.file)
            file_handler.setFormatter(logging.Formatter(logging_config.format))
            logger.addHandler(file_handler)

        return logger

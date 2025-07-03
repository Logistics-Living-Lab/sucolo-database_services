from _typeshed import Incomplete

from sucolo_database_services.elasticsearch_client.service import (
    ElasticsearchService as ElasticsearchService,
)
from sucolo_database_services.redis_client.service import (
    RedisService as RedisService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)
from sucolo_database_services.services.data_management_service import (
    DataManagementService as DataManagementService,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService as DistrictFeaturesService,
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService as DynamicFeaturesService,
)
from sucolo_database_services.services.health_check_service import (
    HealthCheckService as HealthCheckService,
)
from sucolo_database_services.services.metadata_service import (
    MetadataService as MetadataService,
)
from sucolo_database_services.services.multiple_features_service import (
    MultipleFeaturesService as MultipleFeaturesService,
)
from sucolo_database_services.utils.config import Config as Config
from sucolo_database_services.utils.config import LoggingConfig as LoggingConfig

class DataAccess:
    logger: Incomplete
    dynamic_features: Incomplete
    district_features: Incomplete
    data_management: Incomplete
    metadata: Incomplete
    health_check: Incomplete
    multiple_features: Incomplete
    def __init__(self, config: Config) -> None: ...

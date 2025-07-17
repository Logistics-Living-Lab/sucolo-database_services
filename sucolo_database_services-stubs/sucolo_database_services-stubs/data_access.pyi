from logging import Logger

from sucolo_database_services.services.data_management_service import (
    DataManagementService
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService
)
from sucolo_database_services.services.health_check_service import (
    HealthCheckService
)
from sucolo_database_services.services.metadata_service import (
    MetadataService
)
from sucolo_database_services.services.multiple_features_service import (
    MultipleFeaturesService
)
from sucolo_database_services.utils.config import Config as Config


class DataAccess:
    logger: Logger
    dynamic_features: DynamicFeaturesService
    district_features: DistrictFeaturesService
    data_management: DataManagementService
    metadata: MetadataService
    health_check: HealthCheckService
    multiple_features: MultipleFeaturesService
    def __init__(self, config: Config) -> None: ...

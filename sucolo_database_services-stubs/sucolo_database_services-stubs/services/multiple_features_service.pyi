import pandas as pd

from sucolo_database_services.services.base_service import (
    BaseService
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService
)
from sucolo_database_services.services.fields_and_queries import (
    MultipleFeaturesQuery
)
from sucolo_database_services.services.metadata_service import (
    MetadataService
)


class MultipleFeaturesService(BaseService):
    metadata_service: MetadataService
    dynamic_features_service: DynamicFeaturesService
    district_features_service: DistrictFeaturesService

    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
        metadata_service: MetadataService,
        dynamic_features_service: DynamicFeaturesService,
        district_features_service: DistrictFeaturesService,
    ) -> None: ...
    def get_features(self, query: MultipleFeaturesQuery) -> pd.DataFrame: ...

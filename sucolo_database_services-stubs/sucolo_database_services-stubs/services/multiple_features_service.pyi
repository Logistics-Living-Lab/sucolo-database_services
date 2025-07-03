import pandas as pd
from _typeshed import Incomplete

from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService as DistrictFeaturesService,
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService as DynamicFeaturesService,
)
from sucolo_database_services.services.fields_and_queries import (
    MultipleFeaturesQuery as MultipleFeaturesQuery,
)
from sucolo_database_services.services.metadata_service import (
    MetadataService as MetadataService,
)
from sucolo_database_services.utils.exceptions import (
    CityNotFoundError as CityNotFoundError,
)

class MultipleFeaturesService(BaseService):
    metadata_service: Incomplete
    dynamic_features_service: Incomplete
    district_features_service: Incomplete
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
        metadata_service: MetadataService,
        dynamic_features_service: DynamicFeaturesService,
        district_features_service: DistrictFeaturesService,
    ) -> None: ...
    def get_features(self, query: MultipleFeaturesQuery) -> pd.DataFrame: ...

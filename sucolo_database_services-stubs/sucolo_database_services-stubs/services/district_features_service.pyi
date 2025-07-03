import pandas as pd

from sucolo_database_services.services.base_service import (
    BaseService as BaseService,
)
from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)

class DistrictFeaturesService(BaseService):
    def __init__(
        self, base_service_dependencies: BaseServiceDependencies
    ) -> None: ...
    def get_hexagon_district_features(
        self, city: str, feature_columns: list[str], resolution: int
    ) -> pd.DataFrame: ...

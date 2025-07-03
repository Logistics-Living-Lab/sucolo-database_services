import pandas as pd

from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)


class DistrictFeaturesService(BaseService):
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super(DistrictFeaturesService, self).__init__(base_service_dependencies)

    def get_hexagon_district_features(
        self,
        city: str,
        feature_columns: list[str],
        resolution: int,
    ) -> pd.DataFrame:
        """Get static features for hexagons.

        Args:
            city: City name
            feature_columns: List of feature columns to retrieve

        Returns:
            DataFrame containing the requested features
        """
        district_data = self._es_service.read.get_hexagons(
            index_name=city,
            features=feature_columns,
            resolution=resolution,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(
            columns=[
                col
                for col in df.columns
                if col in ["hex_id", "type", "location", "polygon"]
            ]
        )
        return df

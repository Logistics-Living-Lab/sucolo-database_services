import pandas as pd

from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService,
)
from sucolo_database_services.services.dynamic_features_service import (
    DynamicFeaturesService,
)
from sucolo_database_services.services.fields_and_queries import (
    MultipleFeaturesQuery,
)
from sucolo_database_services.services.metadata_service import MetadataService
from sucolo_database_services.utils.exceptions import CityNotFoundError


class MultipleFeaturesService(BaseService):
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
        metadata_service: MetadataService,
        dynamic_features_service: DynamicFeaturesService,
        district_features_service: DistrictFeaturesService,
    ) -> None:
        super().__init__(base_service_dependencies)
        self.metadata_service = metadata_service
        self.dynamic_features_service = dynamic_features_service
        self.district_features_service = district_features_service

    def get_features(self, query: MultipleFeaturesQuery) -> pd.DataFrame:
        """Get multiple features for a given city based on the query parameters.

        This method combines different types of features (nearest distances,
        counts, presences, and hexagon features) into a single DataFrame.

        Args:
            query: DataQuery object containing the query parameters

        Returns:
            DataFrame containing all requested features indexed by hex_id
        """
        # Validate city exists
        if query.city not in self.metadata_service.get_cities():
            raise CityNotFoundError(f"City {query.city} not found")

        index = pd.Index(
            self._es_service.read.get_hexagons(
                index_name=query.city,
                resolution=query.resolution,
                features=[],
                only_location=True,
            ).keys()
        )
        df = pd.DataFrame(index=index)

        # Process nearest distances
        for subquery in query.nearest_queries:
            nearest_feature = (
                self.dynamic_features_service.calculate_nearest_distances(
                    query=subquery
                )
            )
            df = df.join(
                pd.Series(
                    nearest_feature,
                    name="nearest_" + subquery.amenity,
                )
            )

        # Process counts
        for subquery in query.count_queries:
            count_feature = (
                self.dynamic_features_service.count_pois_in_distance(
                    query=subquery,
                )
            )
            df = df.join(
                pd.Series(
                    count_feature,
                    name="count_" + subquery.amenity,
                )
            )

        # Process presences
        for subquery in query.presence_queries:
            presence_feature = (
                self.dynamic_features_service.determine_presence_in_distance(
                    query=subquery,
                )
            )
            df = df.join(
                pd.Series(
                    presence_feature,
                    name="present_" + subquery.amenity,
                )
            )

        # Process hexagon features
        if query.hexagons is not None:
            hexagon_features = (
                self.district_features_service.get_hexagon_district_features(
                    city=query.city,
                    resolution=query.resolution,
                    feature_columns=query.hexagons.features,
                )
            )
            df = df.join(hexagon_features)

        return df

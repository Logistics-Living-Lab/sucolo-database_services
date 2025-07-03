import pandas as pd

from sucolo_database_services.redis_client.consts import POIS_SUFFIX
from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)


class MetadataService(BaseService):
    """Service for static feature types (features that don't require
    any calculations):
    - available cities,
    - available amenities (for selected city),
    - city district attributes."""

    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super(MetadataService, self).__init__(base_service_dependencies)

    def get_cities(self) -> list[str]:
        """Get list of all available cities."""

        cities = self._es_service.get_all_indices()
        cities = list(filter(lambda city: city[0] != ".", cities))
        return cities

    def city_data_exists(self, city: str) -> bool:
        """Check if city data exists in Elasticsearch."""
        return self._es_service.index_manager.index_exists(city)

    def get_amenities(self, city: str) -> list[str]:
        """Get list of all amenities for a given city."""

        city_keys = self._redis_service.keys_manager.get_city_keys(city)
        poi_keys = list(
            filter(
                lambda key: key[-len(POIS_SUFFIX) :] == POIS_SUFFIX,
                city_keys,
            )
        )
        amenities = list(
            map(
                lambda key: key[len(city) + 1 : -len(POIS_SUFFIX)],
                poi_keys,
            )
        )
        return amenities

    def get_district_attributes(self, city: str) -> list[str]:
        """Get list of all district attributes for a given city."""

        district_data = self._es_service.read.get_districts(
            index_name=city,
        )
        df = pd.DataFrame.from_dict(district_data, orient="index")
        df = df.drop(columns=["district", "polygon", "type"], errors="ignore")
        df = df.dropna()  # get features only available for all districts
        district_attributes = list(df.columns)
        return district_attributes

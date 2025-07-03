from pytest_mock import MockerFixture as MockerFixture

from sucolo_database_services.data_access import DataAccess as DataAccess
from sucolo_database_services.services.fields_and_queries import (
    AmenityFields as AmenityFields,
)
from sucolo_database_services.services.fields_and_queries import (
    AmenityQuery as AmenityQuery,
)
from sucolo_database_services.services.fields_and_queries import (
    DistrictFeatureFields as DistrictFeatureFields,
)
from sucolo_database_services.services.fields_and_queries import (
    MultipleFeaturesQuery as MultipleFeaturesQuery,
)
from sucolo_database_services.utils.exceptions import (
    CityNotFoundError as CityNotFoundError,
)

def test_city_not_found(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
def test_invalid_radius() -> None: ...
def test_invalid_penalty() -> None: ...
def test_get_all_indices(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
def test_get_hexagon_static_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
def test_error_handling(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...
def test_get_multiple_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None: ...

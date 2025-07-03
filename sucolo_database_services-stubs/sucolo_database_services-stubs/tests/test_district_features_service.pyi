import pytest
from pytest_mock import MockerFixture as MockerFixture

from sucolo_database_services.services.base_service import (
    BaseServiceDependencies as BaseServiceDependencies,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService as DistrictFeaturesService,
)

@pytest.fixture
def base_service_dependencies() -> BaseServiceDependencies: ...
@pytest.fixture
def district_features_service(
    mocker: MockerFixture, base_service_dependencies: BaseServiceDependencies
) -> DistrictFeaturesService: ...
def test_get_hexagon_district_features_returns_dataframe(
    district_features_service: DistrictFeaturesService, mocker: MockerFixture
) -> None: ...

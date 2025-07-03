from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from sucolo_database_services.services.base_service import (
    BaseServiceDependencies,
)
from sucolo_database_services.services.district_features_service import (
    DistrictFeaturesService,
)


@pytest.fixture
def base_service_dependencies() -> BaseServiceDependencies:
    deps = MagicMock(spec=BaseServiceDependencies)
    deps.logger = MagicMock()
    deps.es_service = MagicMock()
    deps.redis_service = MagicMock()
    return deps


@pytest.fixture
def district_features_service(
    mocker: MockerFixture, base_service_dependencies: BaseServiceDependencies
) -> DistrictFeaturesService:
    service = DistrictFeaturesService(base_service_dependencies)
    service._es_service = mocker.MagicMock()
    return service


def test_get_hexagon_district_features_returns_dataframe(
    district_features_service: DistrictFeaturesService,
    mocker: MockerFixture,
) -> None:
    city = "testcity"
    feature_columns = ["population", "area"]
    resolution = 9
    # Mocked data returned by es_service.read.get_hexagons
    mock_data = {
        "hex1": {
            "population": 100,
            "area": 50,
            "hex_id": "hex1",
            "type": "hex",
            "location": [0, 0],
            "polygon": None,
        },
        "hex2": {
            "population": 200,
            "area": 60,
            "hex_id": "hex2",
            "type": "hex",
            "location": [1, 1],
            "polygon": None,
        },
    }

    mock_get_hexagons = mocker.patch.object(
        district_features_service._es_service.read,
        "get_hexagons",
        return_value=mock_data,
    )

    df = district_features_service.get_hexagon_district_features(
        city=city, feature_columns=feature_columns, resolution=resolution
    )

    # Should be a DataFrame with only 'population' and 'area' columns
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["population", "area"]
    assert set(df.index) == {"hex1", "hex2"}
    assert df.loc["hex1", "population"] == 100
    assert df.loc["hex2", "area"] == 60
    # Should call the underlying es_service.read.get_hexagons
    mock_get_hexagons.assert_called_once_with(
        index_name=city,
        features=feature_columns,
        resolution=resolution,
    )

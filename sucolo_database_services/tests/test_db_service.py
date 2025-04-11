import pandas as pd
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from sucolo_database_services.db_service import (
    AmenityQuery,
    DataQuery,
    DBService,
    HexagonQuery,
)
from sucolo_database_services.utils.exceptions import CityNotFoundError


def test_city_not_found(db_service: DBService, mocker: MockerFixture) -> None:
    # Mock get_cities to return empty list
    mocker.patch.object(
        db_service,
        "get_cities",
        return_value=[],
    )

    with pytest.raises(CityNotFoundError):
        db_service.get_multiple_features(
            DataQuery(
                city="nonexistent",
                nearests=[AmenityQuery(amenity="shop", radius=1000)],
            )
        )


def test_invalid_radius() -> None:
    with pytest.raises(ValidationError):
        AmenityQuery(amenity="shop", radius=-1)


def test_invalid_penalty() -> None:
    with pytest.raises(ValidationError):
        AmenityQuery(amenity="shop", radius=1000, penalty=-1)


def test_get_all_indices(db_service: DBService, mocker: MockerFixture) -> None:
    # Mock Elasticsearch service
    mocker.patch.object(
        db_service.es_service,
        "get_all_indices",
        return_value=["city1", "city2"],
    )

    db_service.get_cities()


def test_get_hexagon_static_features(
    db_service: DBService, mocker: MockerFixture
) -> None:
    # Mock the get_hexagon_static_features method
    mocker.patch.object(
        db_service.es_service.read,
        "get_hexagons",
        return_value={
            "hex1": {
                "type": "hexagon",
                "polygon": {
                    "type": "Polygon",
                    "coordinates": [
                        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
                    ],
                },
                "Employed income": 10000,
                "Average age": 30,
            },
            "hex2": {
                "type": "hexagon",
                "polygon": {
                    "type": "Polygon",
                    "coordinates": [
                        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
                    ],
                },
                "Employed income": 20000,
                "Average age": 40,
            },
            "hex3": {
                "type": "hexagon",
                "polygon": {
                    "type": "Polygon",
                    "coordinates": [
                        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
                    ],
                },
                "Employed income": 30000,
                "Average age": 50,
            },
        },
    )

    feature_columns = ["Employed income", "Average age"]

    # Call the method
    result = db_service.get_hexagon_static_features(
        city="leipzig",
        feature_columns=feature_columns,
    )

    # Verify the result is a DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result.columns) == len(feature_columns)
    assert result.columns.isin(feature_columns).all()


def test_error_handling(db_service: DBService, mocker: MockerFixture) -> None:
    # Mock Redis service to raise an error
    mocker.patch.object(
        db_service.redis_service.keys_manager,
        "get_city_keys",
        side_effect=Exception("Test error"),
    )

    with pytest.raises(Exception):
        db_service.get_amenities("city1")


def test_get_multiple_features(
    db_service: DBService, mocker: MockerFixture
) -> None:
    # Mock the get_cities method to return the test city
    mocker.patch.object(
        db_service,
        "get_cities",
        return_value=["leipzig"],
    )

    # Create test query
    query = DataQuery(
        city="leipzig",
        nearests=[
            AmenityQuery(amenity="education", radius=500, penalty=100),
            AmenityQuery(amenity="hospital", radius=1000),
        ],
        counts=[
            AmenityQuery(amenity="local_business", radius=300),
        ],
        presences=[
            AmenityQuery(amenity="station", radius=200),
        ],
        hexagons=HexagonQuery(features=["Employed income", "Average age"]),
    )

    # Mock the necessary service methods
    mock_calculate_nearests_distances = mocker.patch.object(
        db_service,
        "calculate_nearests_distances",
        return_value={},
    )
    mock_count_pois_in_distance = mocker.patch.object(
        db_service,
        "count_pois_in_distance",
        return_value={},
    )
    mock_determine_presence_in_distance = mocker.patch.object(
        db_service,
        "determine_presence_in_distance",
        return_value={},
    )
    mock_get_hexagon_static_features = mocker.patch.object(
        db_service,
        "get_hexagon_static_features",
        return_value=pd.DataFrame(),
    )

    # Call the method
    result = db_service.get_multiple_features(query)

    # Verify the result is a DataFrame
    assert isinstance(result, pd.DataFrame)

    # Verify each service method was called with correct parameters
    mock_calculate_nearests_distances.assert_called()
    mock_count_pois_in_distance.assert_called()
    mock_determine_presence_in_distance.assert_called()
    mock_get_hexagon_static_features.assert_called()

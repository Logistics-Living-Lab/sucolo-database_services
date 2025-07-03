import pandas as pd
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from sucolo_database_services.data_access import DataAccess
from sucolo_database_services.services.fields_and_queries import (
    AmenityFields,
    AmenityQuery,
    DistrictFeatureFields,
    MultipleFeaturesQuery,
)
from sucolo_database_services.utils.exceptions import CityNotFoundError


def test_city_not_found(data_access: DataAccess, mocker: MockerFixture) -> None:
    # Mock get_cities to return empty list
    mocker.patch.object(
        data_access.metadata,
        "get_cities",
        return_value=[],
    )

    with pytest.raises(CityNotFoundError):
        data_access.multiple_features.get_features(
            MultipleFeaturesQuery(
                city="nonexistent",
                resolution=9,
                nearests=[
                    AmenityQuery(
                        city="leipzig",
                        resolution=9,
                        amenity="shop",
                        radius=1000,
                    )
                ],
            )
        )


def test_invalid_radius() -> None:
    with pytest.raises(ValidationError):
        AmenityQuery(city="leipzig", resolution=9, amenity="shop", radius=-1)


def test_invalid_penalty() -> None:
    with pytest.raises(ValidationError):
        AmenityQuery(
            city="leipzig",
            resolution=9,
            amenity="shop",
            radius=1000,
            penalty=-1,
        )


def test_get_all_indices(
    data_access: DataAccess, mocker: MockerFixture
) -> None:
    # Mock Elasticsearch service
    mocker.patch.object(
        data_access._es_service,
        "get_all_indices",
        return_value=["city1", "city2"],
    )

    data_access.metadata.get_cities()


def test_get_hexagon_static_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None:
    # Mock the get_hexagon_static_features method
    mocker.patch.object(
        data_access._es_service.read,
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
    result = data_access.district_features.get_hexagon_district_features(
        city="leipzig",
        resolution=9,
        feature_columns=feature_columns,
    )

    # Verify the result is a DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result.columns) == len(feature_columns)
    assert result.columns.isin(feature_columns).all()


def test_error_handling(data_access: DataAccess, mocker: MockerFixture) -> None:
    # Mock Redis service to raise an error
    mocker.patch.object(
        data_access._redis_service.keys_manager,
        "get_city_keys",
        side_effect=Exception("Test error"),
    )

    with pytest.raises(Exception):
        data_access.metadata.get_amenities("city1")


def test_get_multiple_features(
    data_access: DataAccess, mocker: MockerFixture
) -> None:
    # Create test query
    query = MultipleFeaturesQuery(
        city="leipzig",
        resolution=9,
        nearests=[
            AmenityFields(amenity="education", radius=500, penalty=100),
            AmenityFields(amenity="hospital", radius=1000),
        ],
        counts=[
            AmenityFields(amenity="local_business", radius=300),
        ],
        presences=[
            AmenityFields(amenity="station", radius=200),
        ],
        hexagons=DistrictFeatureFields(
            features=["Employed income", "Average age"]
        ),
    )

    # Mock the necessary service methods
    mock_get_cities = mocker.patch.object(
        data_access.metadata,
        "get_cities",
        return_value=["leipzig"],
    )
    mock_get_hexagons = mocker.patch.object(
        data_access._es_service.read,
        "get_hexagons",
        return_value={
            "8963b10664bffff": {
                "hex_id": "8963b10664bffff",
                "location": {
                    "lon": 12.425527217738386,
                    "lat": 51.382521836344374,
                },
            },
            "8963b10625bffff": {
                "hex_id": "8963b10625bffff",
                "location": {"lon": 12.4097606740623, "lat": 51.38639802985562},
            },
            "8963b1071d7ffff": {
                "hex_id": "8963b1071d7ffff",
                "location": {
                    "lon": 12.403503987504541,
                    "lat": 51.377776335191605,
                },
            },
        },
    )
    mock_calculate_nearests_distances = mocker.patch.object(
        data_access.dynamic_features,
        "calculate_nearest_distances",
        return_value={
            "8963b10664bffff": 150,
            "8963b10625bffff": 100,
            "8963b1071d7ffff": 200,
        },
    )
    mock_count_pois_in_distance = mocker.patch.object(
        data_access.dynamic_features,
        "count_pois_in_distance",
        return_value={
            "8963b10664bffff": 10,
            "8963b10625bffff": 5,
            "8963b1071d7ffff": 15,
        },
    )
    mock_determine_presence_in_distance = mocker.patch.object(
        data_access.dynamic_features,
        "determine_presence_in_distance",
        return_value={
            "8963b10664bffff": 1,
            "8963b10625bffff": 0,
            "8963b1071d7ffff": 1,
        },
    )
    mock_get_hexagon_district_features = mocker.patch.object(
        data_access.district_features,
        "get_hexagon_district_features",
        return_value=pd.DataFrame(
            {
                "Employed income": [10000, 20000, 30000],
                "Average age": [30, 40, 50],
            },
            index=pd.Index(
                ["8963b10664bffff", "8963b10625bffff", "8963b1071d7ffff"],
                name="hex_id",
            ),
        ),
    )

    # Call the method
    df = data_access.multiple_features.get_features(query)

    # Verify the result is a DataFrame
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (3, 6)
    assert df.columns.isin(
        [
            "nearest_education",
            "nearest_hospital",
            "count_local_business",
            "present_station",
            "Employed income",
            "Average age",
        ]
    ).all()
    assert df["nearest_education"].tolist() == [150, 100, 200]
    assert df["nearest_hospital"].tolist() == [150, 100, 200]
    assert df["count_local_business"].tolist() == [10, 5, 15]
    assert df["present_station"].tolist() == [1, 0, 1]
    assert df["Employed income"].tolist() == [10000, 20000, 30000]
    assert df["Average age"].tolist() == [30, 40, 50]

    # Verify each service method was called with correct parameters
    mock_get_cities.assert_called()
    mock_get_hexagons.assert_called()
    mock_calculate_nearests_distances.assert_called()
    mock_count_pois_in_distance.assert_called()
    mock_determine_presence_in_distance.assert_called()
    mock_get_hexagon_district_features.assert_called()

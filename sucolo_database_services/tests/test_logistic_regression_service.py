import math

from sucolo_database_services.services.logistic_regression_service import (
    logistic_regression,
    score_hexagons_with_selected_features,
)


def test_logistic_regression_returns_neutral_for_invalid_payload() -> None:
    assert logistic_regression(None) == 0.5
    assert logistic_regression([]) == 0.5


def test_logistic_regression_returns_score_in_range() -> None:
    features = [
        {
            "name": "Free Term",
            "type": "district",
            "value": 1,
            "weight": -0.2,
        },
        {
            "name": "education",
            "type": "nearest",
            "value": 250,
            "radius": 500,
            "penalty": 100,
            "weight": 0.8,
        },
        {
            "name": "station",
            "type": "present",
            "value": 1,
            "weight": 0.4,
        },
        {
            "name": "Slope class",
            "type": "district",
            "value": 3,
            "weight": 0.6,
        },
        {
            "name": "Employed income",
            "type": "district",
            "value": 5000,
            "weight": 0.3,
        },
    ]

    score = logistic_regression(features=features, resolution=9)

    assert 0 <= score <= 1
    assert not math.isnan(score)


def test_logistic_regression_slope_penalty_effect() -> None:
    base_features = [
        {
            "name": "Free Term",
            "type": "district",
            "value": 1,
            "weight": 0.0,
        },
        {
            "name": "Slope class",
            "type": "district",
            "value": 2,
            "weight": 0.6,
        },
    ]
    steep_slope_features = [
        {
            "name": "Free Term",
            "type": "district",
            "value": 1,
            "weight": 0.0,
        },
        {
            "name": "Slope class",
            "type": "district",
            "value": 5,
            "weight": 0.6,
        },
    ]

    score_base = logistic_regression(features=base_features, resolution=9)
    score_steep = logistic_regression(features=steep_slope_features, resolution=9)

    assert score_steep < score_base


def test_score_hexagons_with_selected_features_returns_raw_values() -> None:
    hexagon_feature_values = {
        "8a63b10586cffff": {
            "nearest_station": 1200,
            "present_education": 0,
            "Slope class": 3,
            "Households with 3 people": 290,
        },
        "8a63b10586dffff": {
            "nearest_station": 450,
            "present_education": 1,
            "Slope class": 2,
            "Households with 3 people": 340,
        },
    }
    selected_features = [
        {"name": "Free Term", "type": "district", "weight": -0.2, "value": 1},
        {
            "name": "nearest_station",
            "type": "nearest",
            "weight": 0.8,
            "radius": 1000,
            "penalty": 200,
        },
        {"name": "present_education", "type": "present", "weight": 0.4},
        {"name": "Slope class", "type": "district", "weight": 0.6},
        {
            "name": "Households with 3 people",
            "type": "district",
            "weight": 0.2,
        },
    ]

    results = score_hexagons_with_selected_features(
        hexagon_feature_values=hexagon_feature_values,
        selected_features=selected_features,
        resolution=9,
    )

    assert len(results) == 2
    for result in results:
        assert "hexId" in result
        assert "score" in result
        assert "rawFeatureValues" in result
        assert 0 <= result["score"] <= 1
        assert not math.isnan(result["score"])
        assert "nearest_station" in result["rawFeatureValues"]
        assert "present_education" in result["rawFeatureValues"]
        assert "Slope class" in result["rawFeatureValues"]
        assert "Households with 3 people" in result["rawFeatureValues"]
        assert "Free Term" not in result["rawFeatureValues"]

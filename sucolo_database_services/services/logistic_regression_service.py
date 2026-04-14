import math
from typing import Any

RESOLUTION_TO_RADIUS = {
    7: 1406,
    8: 531,
    9: 200,
    10: 65,
}
MAX_COUNT = 5
LINEAR_SCALING_FACTOR = 2

DISTRICT_THRESHOLDS = {
    "Average age": 100,
    "Employed income": 10000,
    "Gross monthly wages": 10000,
    "Household income": 10000,
    "Other income": 10000,
    "Pensions": 10000,
    "Unemployment benefits": 10000,
    "Households with 1 person": 1000,
    "Households with 2 people": 1000,
    "Households with 3 people": 1000,
    "Households with 4 people": 1000,
    "Households with 5 or more people": 1000,
    "Housing": 1000,
    "Population density": 10000,
    "Total employed": 10000,
    "Total population": 10000,
    "Total unemployed": 10000,
}

SLOPE_CLASS_TO_SCORE = {
    1: 1.0,
    2: 0.8,
    3: 0.4,
    4: 0.2,
    5: 0.0,
}


def _is_free_term(feature: dict[str, Any]) -> bool:
    return feature.get("name") == "Free Term" or feature.get("label") == "Free Term"


def _to_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        numeric_value = float(value)
        if math.isnan(numeric_value):
            return None
        return numeric_value
    return None


def _normalize_value(feature: dict[str, Any], hex_radius: float) -> float:
    if _is_free_term(feature):
        return 1.0

    value = _to_numeric(feature.get("value"))
    if value is None:
        return 0.0

    feature_type = feature.get("type")

    if feature_type == "nearest":
        min_radius = hex_radius
        radius = _to_numeric(feature.get("radius")) or 0.0
        penalty = _to_numeric(feature.get("penalty")) or 0.0
        max_radius = max(min_radius + 1e-6, radius + penalty)
        return max(0.0, min(1.0, 1 - ((value - min_radius) / (max_radius - min_radius))))

    if feature_type == "count":
        return min(1.0, value / MAX_COUNT)

    if feature_type == "present":
        return 1.0 if bool(value) else 0.0

    if feature_type == "district":
        name = str(feature.get("name") or "")
        lower_name = name.lower()
        if name == "Slope class" or lower_name == "slope class" or "slope" in lower_name:
            slope_class = round(value)
            return SLOPE_CLASS_TO_SCORE.get(slope_class, 0.0)

        divisor = DISTRICT_THRESHOLDS.get(name, 10000)
        return min(1.0, max(0.0, value / divisor))

    return 0.0


def logistic_regression(
    features: list[dict[str, Any]] | None,
    resolution: int = 9,
) -> float:
    """Compute logistic regression score from pre-weighted feature payload."""
    if not isinstance(features, list) or len(features) == 0:
        return 0.5

    hex_radius = RESOLUTION_TO_RADIUS.get(resolution, RESOLUTION_TO_RADIUS[9])
    processed: list[dict[str, Any]] = []

    for feature in features:
        if not isinstance(feature, dict):
            continue
        weight = _to_numeric(feature.get("weight")) or 0.0
        normalized_value = _normalize_value(feature, float(hex_radius))
        processed.append(
            {
                **feature,
                "weight": weight,
                "normalizedValue": normalized_value,
            }
        )

    if len(processed) == 0:
        return 0.5

    bias = 0.0
    weighted_sum = 0.0
    count = 0
    slope_penalty = 0.0

    for feature in processed:
        if _is_free_term(feature):
            bias = float(feature["weight"])
            continue

        name = feature.get("name")
        feature_type = feature.get("type")

        if (
            feature_type == "district"
            and isinstance(name, str)
            and "slope" in name.lower()
        ):
            raw_class = _to_numeric(feature.get("value")) or 0.0
            slope_class = round(raw_class)
            if slope_class >= 5:
                slope_penalty -= 0.8
            elif slope_class == 4:
                slope_penalty -= 0.6
            elif slope_class == 3:
                slope_penalty -= 0.2

        weighted_sum += float(feature["weight"]) * float(feature["normalizedValue"])
        count += 1

    scaled_sum = weighted_sum / max(1, count)
    total = (scaled_sum + bias + slope_penalty) * LINEAR_SCALING_FACTOR
    score = 1 / (1 + math.exp(-total))
    return max(0.0, min(1.0, score))


def score_hexagons_with_selected_features(
    hexagon_feature_values: dict[str, dict[str, Any]],
    selected_features: list[dict[str, Any]] | None,
    resolution: int = 9,
) -> list[dict[str, Any]]:
   
    """Calculate logistic regression per hexagon 

    Args:
        hexagon_feature_values: Mapping of hex_id to available feature values.
        selected_features: Feature definitions containing at least name/type/weight.
            Optional "column" key can map a feature to a raw value key.
        resolution: H3 resolution used by nearest feature normalization.

    Returns:
        List of per-hexagon scoring results:
        {
            "hexId": "...",
            "score": <float>,
            "rawFeatureValues": { ...selected raw values... },
        }
    """
    if (
        not isinstance(hexagon_feature_values, dict)
        or not isinstance(selected_features, list)
        or len(selected_features) == 0
    ):
        return []

    results: list[dict[str, Any]] = []
    for hex_id, row in hexagon_feature_values.items():
        if not isinstance(hex_id, str) or not isinstance(row, dict):
            continue

        regression_features: list[dict[str, Any]] = []
        raw_feature_values: dict[str, Any] = {}

        for selected_feature in selected_features:
            if not isinstance(selected_feature, dict):
                continue

            column_name = str(
                selected_feature.get("column")
                or selected_feature.get("name")
                or ""
            )
            if column_name == "":
                continue

            feature_for_score = {**selected_feature}
            if _is_free_term(selected_feature):
                feature_for_score["value"] = selected_feature.get("value", 1)
            else:
                raw_value = row.get(column_name)
                raw_feature_values[column_name] = raw_value
                feature_for_score["value"] = raw_value

            regression_features.append(feature_for_score)

        score = logistic_regression(
            features=regression_features,
            resolution=resolution,
        )
        results.append(
            {
                "hexId": hex_id,
                "score": score,
                "rawFeatureValues": raw_feature_values,
            }
        )

    return results

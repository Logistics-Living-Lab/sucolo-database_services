# Sucolo Database Services

A Python package providing database services for the Sucolo project, including Elasticsearch and Redis clients with additional utilities for data processing and analysis.

## Features

- Elasticsearch client integration
- Redis client integration
- Data processing utilities
- H3 geospatial indexing support
- Type-safe data handling with Pydantic

## Requirements

- Python 3.10, 3.11, 3.12, 3.13
- Poetry for dependency management

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/sucolo-database_services.git
cd sucolo-database_services
```

1. Install dependencies using Poetry:

```bash
poetry install
```

1. Set up your environment variables in `.env` file:

```bash
# Example .env configuration
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
REDIS_HOST=localhost
REDIS_PORT=6379
```

## Development

### Code Style

This project uses several tools to maintain code quality:

- Black for code formatting
- Flake8 for linting
- MyPy for type checking
- isort for import sorting

Run the following command to format and check the code:

```bash
make format
```

### Testing

Run tests using pytest:

```bash
make test
```

## Logistic Regression API

The regression endpoint computes a logistic score from user-provided features.

- **Method:** `POST`
- **Path:** `/cities/<city>/regression/logistic`
- **Body:**
  - `features` (required): list of feature objects
  - `resolution` (optional): integer, default `9`
- **Success response:** `{ "score": <float in [0, 1]> }`

Example request payload:

```json
{
  "resolution": 9,
  "features": [
    { "name": "Free Term", "type": "district", "value": 1, "weight": -0.2 },
    { "name": "education", "type": "nearest", "value": 250, "radius": 500, "penalty": 100, "weight": 0.8 },
    { "name": "station", "type": "present", "value": 1, "weight": 0.4 },
    { "name": "Slope class", "type": "district", "value": 3, "weight": 0.6 }
  ]
}
```

Validation errors:

- If `features` is not a list: `400` with `{ "error": "\"features\" must be a list." }`
- If `resolution` is not an integer: `400` with `{ "error": "\"resolution\" must be an integer." }`

Implementation details:

- Invalid or empty `features` input returns the neutral score `0.5`
- Feature types supported: `nearest`, `count`, `present`, `district`
- `"Free Term"` is treated as bias
- Slope class applies both class scoring and an additional steep-slope penalty
- Final score is a sigmoid output clamped to `[0, 1]`

## Per-Hexagon Regression (Backend Wiring)

Per-hexagon scoring should be exposed from the backend repository
(`src/app/routes/regression.py`), while this package provides the scoring logic.

Recommended endpoint:

- **Method:** `POST`
- **Path:** `/cities/<city>/regression/logistic/hexagons`
- **Body:** selected dynamic and district features with weights
- **Response:** one score per hexagon with selected raw values

Example response item:

```json
{
  "hexId": "8a63b10586cffff",
  "score": 0.40236120042833506,
  "rawFeatureValues": {
    "Households with 3 people": 290,
    "Households with 4 people": 207,
    "Households with 5 or more people": 57,
    "nearest_cafe": 1001,
    "nearest_entertainment": 1001,
    "nearest_local_business": 1001,
    "nearest_restaurant": 1000,
    "nearest_station": 1200,
    "nearest_supermarket": 1001,
    "present_education": 0,
    "present_gas_station": 0
  }
}
```

### Backend route handler sketch

Add a route in your backend app (`src/app/routes/regression.py`) that:

1. Builds a `MultipleFeaturesQuery` from request body and path `city`
2. Calls `data_access.multiple_features.get_features(...)`
3. Converts DataFrame rows into `hex_id -> feature dict`
4. Calls `score_hexagons_with_selected_features(...)`
5. Returns `results` list

```python
from flask import Blueprint, Response, jsonify, request
from sucolo_database_services.services.fields_and_queries import (
    DistrictFeatureFields,
    MultipleFeaturesQuery,
)
from sucolo_database_services.services.logistic_regression_service import (
    score_hexagons_with_selected_features,
)

bp = Blueprint("regression", __name__, url_prefix="/cities/<city>/regression")


@bp.route("/logistic/hexagons", methods=["POST"])
def calculate_logistic_regression_per_hexagon(
    city: str,
) -> Response | tuple[Response, int]:
    request_dict = request.get_json(silent=True) or {}
    resolution = request_dict.get("resolution", 9)
    selected_features = request_dict.get("selectedFeatures", [])
    nearests = request_dict.get("nearests", [])
    counts = request_dict.get("counts", [])
    presences = request_dict.get("presences", [])
    district_feature_names = request_dict.get("districtFeatureNames", [])

    if not isinstance(resolution, int) or isinstance(resolution, bool):
        return jsonify({"error": '"resolution" must be an integer.'}), 400
    if not isinstance(selected_features, list):
        return jsonify({"error": '"selectedFeatures" must be a list.'}), 400
    if not isinstance(district_feature_names, list):
        return jsonify({"error": '"districtFeatureNames" must be a list.'}), 400

    query = MultipleFeaturesQuery(
        city=city,
        resolution=resolution,
        nearests=nearests,
        counts=counts,
        presences=presences,
        hexagons=DistrictFeatureFields(features=district_feature_names),
    )

    df = data_access.multiple_features.get_features(query=query)
    hexagon_feature_values = {
        str(hex_id): row.dropna().to_dict()
        for hex_id, row in df.iterrows()
    }

    results = score_hexagons_with_selected_features(
        hexagon_feature_values=hexagon_feature_values,
        selected_features=selected_features,
        resolution=resolution,
    )
    return jsonify({"city": city, "resolution": resolution, "results": results})
```

Register the blueprint in backend routes init:

```python
from src.app.routes import regression

app.register_blueprint(regression.bp)
```

## Project Structure

```bash
sucolo_database_services/
├── elasticsearch_client/     # Elasticsearch client implementation
├── redis_client/             # Redis client implementation
├── services/                 # Service layer (feature, metadata, management, etc.)
├── utils/                    # Utility functions and helpers
├── tests/                    # Test suite for all services and modules
├── data_access.py            # Aggregated data access layer
└── __init__.py
```

## Dependencies

Main dependencies:

- elasticsearch
- redis
- pandas
- geopandas
- h3
- pydantic
- python-dotenv

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]

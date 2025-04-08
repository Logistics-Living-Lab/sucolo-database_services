import os
from pathlib import Path

from dotenv import load_dotenv

from sucolo_database_services.db_service import DataQuery, DBService

load_dotenv()
ES_HOST: str = os.environ.get("ELASTIC_HOST", "https://localhost:9200")
ES_USER: str = os.environ.get("ELASTIC_USER", "elastic")
ES_PASSWORD: str = os.environ.get("ELASTIC_PASSWORD", "")
ES_PORT: int = int(os.environ.get("ELASTIC_PORT", 9200))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DATABASE", 0))
CA_CERTS = Path(os.getenv("CA_CERTS", "certs/ca.crt"))


db_service = DBService(
    elastic_host=ES_HOST,
    elastic_user=ES_USER,
    elastic_password=ES_PASSWORD,
    redis_host=REDIS_HOST,
    redis_port=REDIS_PORT,
    redis_db=REDIS_DB,
    ca_certs=CA_CERTS,
)


def test_get_multiple_features() -> None:
    query = {
        "city": "leipzig",
        "nearests": [
            {"amenity": "education", "radius": 500, "penalty": 100},
            {"amenity": "hospital", "radius": 1000},
        ],
        "counts": [{"amenity": "local_business", "radius": 300}],
        "presences": [{"amenity": "station", "radius": 200}],
        "hexagons": {"features": ["Employed income", "Average age"]},
    }
    data = db_service.get_multiple_features(
        DataQuery(**query)  # type: ignore[arg-type]
    )
    data

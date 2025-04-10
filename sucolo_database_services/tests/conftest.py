import os

import pytest
from dotenv import load_dotenv

from sucolo_database_services.db_service import DBService
from sucolo_database_services.utils.config import (
    Config,
    DatabaseConfig,
    Environment,
)

load_dotenv(override=True)


@pytest.fixture
def config() -> Config:
    return Config(
        environment=Environment.DEVELOPMENT,
        database=DatabaseConfig(
            elastic_host=os.getenv("ELASTIC_HOST", "https://localhost:9200"),
            elastic_user=os.getenv("ELASTIC_USER", "elastic"),
            elastic_password=os.getenv("ELASTIC_PASSWORD", "changeme"),
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", 6379)),
            redis_db=int(os.getenv("REDIS_DB", 0)),
        ),
    )


@pytest.fixture
def db_service(config: Config) -> DBService:
    return DBService(config)

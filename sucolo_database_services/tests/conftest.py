import pytest

from sucolo_database_services.data_access import DataAccess
from sucolo_database_services.utils.config import (
    Config,
    DatabaseConfig,
    Environment,
)


@pytest.fixture
def config() -> Config:
    return Config(
        environment=Environment.DEVELOPMENT,
        database=DatabaseConfig(
            elastic_host="https://localhost:9200",
            elastic_user="elastic",
            elastic_password="password",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
        ),
    )


@pytest.fixture
def data_access(config: Config) -> DataAccess:
    return DataAccess(config)

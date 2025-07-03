import pytest

from sucolo_database_services.data_access import DataAccess as DataAccess
from sucolo_database_services.utils.config import Config as Config
from sucolo_database_services.utils.config import (
    DatabaseConfig as DatabaseConfig,
)
from sucolo_database_services.utils.config import Environment as Environment

@pytest.fixture
def config() -> Config: ...
@pytest.fixture
def data_access(config: Config) -> DataAccess: ...

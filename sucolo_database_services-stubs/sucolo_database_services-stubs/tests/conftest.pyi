import pytest

from sucolo_database_services.data_access import DataAccess
from sucolo_database_services.utils.config import Config


@pytest.fixture
def config() -> Config: ...
@pytest.fixture
def data_access(config: Config) -> DataAccess: ...

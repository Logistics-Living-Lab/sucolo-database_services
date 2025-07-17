from elasticsearch import Elasticsearch

from sucolo_database_services.elasticsearch_client.index_manager import (
    ElasticsearchIndexManager
)
from sucolo_database_services.elasticsearch_client.read_repository import (
    ElasticsearchReadRepository
)
from sucolo_database_services.elasticsearch_client.write_repository import (
    ElasticsearchWriteRepository
)


class ElasticsearchService:
    index_manager: ElasticsearchIndexManager
    read: ElasticsearchReadRepository
    write: ElasticsearchWriteRepository
    def __init__(self, es_client: Elasticsearch) -> None: ...
    def get_all_indices(self) -> list[str]: ...
    def check_health(self) -> bool: ...

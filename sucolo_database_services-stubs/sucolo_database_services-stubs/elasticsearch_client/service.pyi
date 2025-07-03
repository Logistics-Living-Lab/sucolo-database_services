from _typeshed import Incomplete
from elasticsearch import Elasticsearch as Elasticsearch

from sucolo_database_services.elasticsearch_client.index_manager import (
    ElasticsearchIndexManager as ElasticsearchIndexManager,
)
from sucolo_database_services.elasticsearch_client.read_repository import (
    ElasticsearchReadRepository as ElasticsearchReadRepository,
)
from sucolo_database_services.elasticsearch_client.write_repository import (
    ElasticsearchWriteRepository as ElasticsearchWriteRepository,
)

class ElasticsearchService:
    index_manager: Incomplete
    read: Incomplete
    write: Incomplete
    def __init__(self, es_client: Elasticsearch) -> None: ...
    def get_all_indices(self) -> list[str]: ...
    def check_health(self) -> bool: ...

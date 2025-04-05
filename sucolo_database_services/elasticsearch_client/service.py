from elasticsearch import Elasticsearch

from sucolo_database_services.elasticsearch_client.index_manager import (
    ElasticsearchIndexManager,
)
from sucolo_database_services.elasticsearch_client.read_repository import (
    ElasticsearchReadRepository,
)
from sucolo_database_services.elasticsearch_client.write_repository import (
    ElasticsearchWriteRepository,
)


class ElasticsearchService:
    def __init__(
        self,
        es_client: Elasticsearch,
    ) -> None:
        self.es = es_client
        self.index_manager = ElasticsearchIndexManager(es_client=es_client)
        self.read = ElasticsearchReadRepository(
            es_client=es_client,
        )
        self.write = ElasticsearchWriteRepository(
            es_client=es_client,
        )

    def get_all_indices(
        self,
    ) -> list[str]:
        return self.es.indices.get_alias(  # type: ignore[return-value]
            index="*"
        )

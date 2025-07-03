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
        self._es_client = es_client
        self.index_manager = ElasticsearchIndexManager(
            es_client=self._es_client
        )
        self.read = ElasticsearchReadRepository(
            es_client=self._es_client,
        )
        self.write = ElasticsearchWriteRepository(
            es_client=self._es_client,
        )

    def get_all_indices(
        self,
    ) -> list[str]:
        return self._es_client.indices.get_alias(  # type: ignore[return-value]
            index="*"
        )

    def check_health(self) -> bool:
        """Check if Elasticsearch is reachable."""
        return self._es_client.ping()

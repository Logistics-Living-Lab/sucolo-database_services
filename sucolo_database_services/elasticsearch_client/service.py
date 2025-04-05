from typing import Any

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
        index_name: str,
        mapping: dict[str, Any],
    ) -> None:
        self.es = es_client
        self.index_name = index_name
        self.mapping = mapping
        self.index_manager = ElasticsearchIndexManager(
            es_client=es_client, index_name=index_name, mapping=mapping
        )
        self.read = ElasticsearchReadRepository(
            es_client=es_client,
            index_name=index_name,
        )
        self.write = ElasticsearchWriteRepository(
            es_client=es_client,
            index_name=index_name,
        )

    def get_all_indices(
        self,
    ) -> list[str]:
        return self.es.indices.get_alias(  # type: ignore[return-value]
            index="*"
        )

    def change_index(
        self,
        new_index: str,
        if_create: bool = False,
        mapping: dict[str, Any] | None = None,
        ignore_if_exists: bool = True,
    ) -> None:
        if not if_create:
            assert self.es.indices.exists(
                index=new_index
            ), f'Index "{new_index}" doesn\'t exist.'

        self.index_name = new_index
        self.index_manager.index_name = new_index
        self.read.index_name = new_index
        self.write.index_name = new_index

        if if_create:
            if mapping is None:
                mapping = self.mapping

            self.index_manager.create_index(ignore_if_exists=ignore_if_exists)

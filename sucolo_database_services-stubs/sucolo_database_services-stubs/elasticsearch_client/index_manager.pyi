from typing import Any

from _typeshed import Incomplete
from elasticsearch import Elasticsearch as Elasticsearch

default_mapping: Incomplete

class ElasticsearchIndexManager:
    es: Incomplete
    def __init__(self, es_client: Elasticsearch) -> None: ...
    def create_index(
        self,
        index_name: str,
        ignore_if_exists: bool = False,
        mapping: dict[str, Any] = ...,
    ) -> None: ...
    def delete_index(
        self, index_name: str, ignore_if_index_not_exist: bool = True
    ) -> None: ...
    def index_exists(self, index_name: str) -> bool: ...

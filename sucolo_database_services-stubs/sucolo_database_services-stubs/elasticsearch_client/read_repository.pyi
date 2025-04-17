from _typeshed import Incomplete
from elasticsearch import Elasticsearch as Elasticsearch

COORD_TYPE = dict[str, float]

class ElasticsearchReadRepository:
    es: Incomplete
    def __init__(self, es_client: Elasticsearch) -> None: ...
    def get_pois(
        self,
        index_name: str,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]: ...
    def get_hexagons(
        self,
        index_name: str,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]: ...
    def get_districts(
        self,
        index_name: str,
        features: list[str] = [],
        only_polygon: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]: ...

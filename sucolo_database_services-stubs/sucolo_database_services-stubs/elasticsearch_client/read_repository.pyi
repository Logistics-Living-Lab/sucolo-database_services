from dataclasses import dataclass, field
from typing import Any

from _typeshed import Incomplete
from elasticsearch import Elasticsearch as Elasticsearch

COORD_TYPE = dict[str, float]
HIT_TYPE = dict[str, Any]

@dataclass
class QueryConstructor:
    type_name: str
    id_name: str | None = ...
    features: list[str] = field(default_factory=Incomplete)
    only_location: bool = ...
    only_polygon: bool = ...
    size: int = ...
    resolution: int | None = ...
    def __post_init__(self) -> None: ...
    def build(self) -> dict[str, Any]: ...

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
        resolution: int,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]: ...
    def get_districts(
        self,
        index_name: str,
        features: list[str] = [],
        only_polygon: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]: ...

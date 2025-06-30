from dataclasses import dataclass, field
from typing import Any

from elasticsearch import Elasticsearch

COORD_TYPE = dict[str, float]
HIT_TYPE = dict[str, Any]


@dataclass
class QueryConstructor:
    type_name: str
    id_name: str | None = None
    features: list[str] = field(default_factory=lambda: [])
    only_location: bool = False
    only_polygon: bool = False
    size: int = 10_000
    resolution: int | None = None

    def __post_init__(self) -> None:
        if self.only_location and self.only_polygon:
            raise ValueError(
                "Cannot set both only_location and only_polygon to True."
            )
        if self.only_location:
            self.features = ["location"]
        elif self.only_polygon:
            self.features = ["polygon"]

    def build(
        self,
    ) -> dict[str, Any]:
        must: list[dict[str, dict[str, int | str]]] = [
            {"term": {"type": self.type_name}}
        ]
        if self.resolution is not None:
            must.append({"term": {"resolution": self.resolution}})
        query = {
            "size": self.size,
            "query": {"bool": {"must": must}},
            "_source": [self.id_name, *self.features],
        }
        if len(self.features) == 0:
            query.pop("_source")

        return query


class ElasticsearchReadRepository:
    def __init__(self, es_client: Elasticsearch):
        self.es = es_client

    def get_pois(
        self,
        index_name: str,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._query(
            QueryConstructor(
                type_name="poi",
                id_name=None,
                features=features,
                only_location=only_location,
            ),
            index_name=index_name,
        )

    def get_hexagons(
        self,
        index_name: str,
        resolution: int,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._query(
            QueryConstructor(
                type_name="hex_center",
                id_name="hex_id",
                resolution=resolution,
                features=features,
                only_location=only_location,
            ),
            index_name=index_name,
        )

    def get_districts(
        self,
        index_name: str,
        features: list[str] = [],
        only_polygon: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._query(
            QueryConstructor(
                type_name="district",
                id_name="district",
                features=features,
                only_polygon=only_polygon,
            ),
            index_name=index_name,
        )

    def _query(
        self,
        query_constructor: QueryConstructor,
        index_name: str,
    ) -> dict[str, dict[str, Any]]:
        query = query_constructor.build()

        response = self.es.search(index=index_name, body=query)

        result = self._postprocess_response(
            hits=response["hits"]["hits"],
            id_name=query_constructor.id_name,
        )

        return result

    def _postprocess_response(
        self,
        hits: list[HIT_TYPE],
        id_name: str | None = None,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        if id_name:
            result = {hit["_source"][id_name]: hit["_source"] for hit in hits}
        else:
            result = {hit["_id"]: hit["_source"] for hit in hits}

        return result

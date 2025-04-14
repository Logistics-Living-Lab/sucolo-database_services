from elasticsearch import Elasticsearch

COORD_TYPE = dict[str, float]


class ElasticsearchReadRepository:
    def __init__(self, es_client: Elasticsearch):
        self.es = es_client

    def get_pois(
        self,
        index_name: str,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._get_geopoints(
            index_name=index_name,
            id_name=None,
            type_name="poi",
            features=features,
            only_location=only_location,
        )

    def get_hexagons(
        self,
        index_name: str,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._get_geopoints(
            index_name=index_name,
            id_name="hex_id",
            type_name="hex_center",
            features=features,
            only_location=only_location,
        )

    def get_districts(
        self,
        index_name: str,
        features: list[str] = [],
        only_polygon: bool = False,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        return self._get_geopolygons(
            index_name=index_name,
            id_name="district",
            type_name="district",
            features=features,
            only_polygon=only_polygon,
        )

    def _get_geopoints(
        self,
        index_name: str,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        only_location: bool = False,
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        if only_location:
            features = ["location"]

        return self._query(
            index_name=index_name,
            id_name=id_name,
            type_name=type_name,
            features=features,
            size=size,
        )

    def _get_geopolygons(
        self,
        index_name: str,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        only_polygon: bool = False,
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        if only_polygon:
            features = ["polygon"]

        return self._query(
            index_name=index_name,
            id_name=id_name,
            type_name=type_name,
            features=features,
            size=size,
        )

    def _query(
        self,
        index_name: str,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float | COORD_TYPE]]:
        query = {
            "size": size,
            "query": {"term": {"type": type_name}},
            "_source": [id_name, *features],
        }
        if len(features) == 0:
            query.pop("_source")

        response = self.es.search(index=index_name, body=query)

        hits = response["hits"]["hits"]
        if id_name:
            result = {hit["_source"][id_name]: hit["_source"] for hit in hits}
        else:
            result = {hit["_id"]: hit["_source"] for hit in hits}

        return result

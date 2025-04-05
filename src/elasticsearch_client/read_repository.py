from elasticsearch import Elasticsearch


class ElasticsearchReadRepository:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name

    def get_pois(
        self,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float]]:
        return self._get_geopoints(
            id_name=None,
            type_name="poi",
            features=features,
            only_location=only_location,
        )

    def get_hexagons(
        self,
        features: list[str] = [],
        only_location: bool = False,
    ) -> dict[str, dict[str, str | int | float]]:
        return self._get_geopoints(
            id_name="hex_id",
            type_name="hex_center",
            features=features,
            only_location=only_location,
        )

    def get_districts(
        self,
        features: list[str] = [],
        only_polygon: bool = False,
    ) -> dict[str, dict[str, str | int | float]]:
        return self._get_geopolygons(
            id_name="name",
            type_name="district",
            features=features,
            only_polygon=only_polygon,
        )

    def _get_geopoints(
        self,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        only_location: bool = False,
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float]]:
        if only_location:
            features = ["location"]

        return self._query(
            id_name=id_name,
            type_name=type_name,
            features=features,
            size=size,
        )

    def _get_geopolygons(
        self,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        only_polygon: bool = False,
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float]]:
        if only_polygon:
            features = ["polygon"]

        return self._query(
            id_name=id_name,
            type_name=type_name,
            features=features,
            size=size,
        )

    def _query(
        self,
        type_name: str,
        id_name: str | None = None,
        features: list[str] = [],
        size: int = 10_000,
    ) -> dict[str, dict[str, str | int | float]]:
        query = {
            "size": size,
            "query": {"term": {"type": type_name}},
            "_source": [id_name, *features],
        }
        if len(features) == 0:
            query.pop("_source")

        response = self.es.search(index=self.index_name, body=query)

        hits = response["hits"]["hits"]
        if id_name:
            result = {hit["_source"][id_name]: hit["_source"] for hit in hits}
        else:
            result = {hit["_id"]: hit["_source"] for hit in hits}

        return result

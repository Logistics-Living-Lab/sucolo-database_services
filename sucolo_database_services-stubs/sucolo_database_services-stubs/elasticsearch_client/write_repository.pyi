import geopandas as gpd
from elasticsearch import Elasticsearch


class ElasticsearchWriteRepository:
    es: Elasticsearch
    def __init__(self, es_client: Elasticsearch) -> None: ...

    def upload_pois(
        self,
        index_name: str,
        gdf: gpd.GeoDataFrame,
        extra_features: list[str] = [],
    ) -> None: ...

    def upload_districts(
        self, index_name: str, gdf: gpd.GeoDataFrame
    ) -> None: ...

    def upload_hex_centers(
        self, index_name: str, districts: gpd.GeoDataFrame, hex_resolution: int
    ) -> None: ...

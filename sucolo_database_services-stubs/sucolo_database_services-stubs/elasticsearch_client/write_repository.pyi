import geopandas as gpd
from _typeshed import Incomplete
from elasticsearch import Elasticsearch as Elasticsearch

from sucolo_database_services.utils.polygons2hexagons import (
    polygons2hexagons as polygons2hexagons,
)

class ElasticsearchWriteRepository:
    es: Incomplete
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

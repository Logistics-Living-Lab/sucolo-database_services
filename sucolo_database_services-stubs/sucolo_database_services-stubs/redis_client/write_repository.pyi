import geopandas as gpd
from _typeshed import Incomplete
from redis import Redis as Redis
from redis.typing import ResponseT as ResponseT

from sucolo_database_services.redis_client.consts import (
    HEX_SUFFIX as HEX_SUFFIX,
)
from sucolo_database_services.redis_client.consts import (
    POIS_SUFFIX as POIS_SUFFIX,
)
from sucolo_database_services.utils.polygons2hexagons import (
    polygons2hexagons as polygons2hexagons,
)

class RedisWriteRepository:
    redis_client: Incomplete
    def __init__(self, redis_client: Redis) -> None: ...
    def upload_pois_by_amenity_key(
        self,
        city: str,
        pois: gpd.GeoDataFrame,
        only_wheelchair_accessible: bool = False,
        wheelchair_positive_values: list[str] = ["yes"],
    ) -> list[int]: ...
    def upload_hex_centers(
        self, city: str, districts: gpd.GeoDataFrame, resolution: int = 9
    ) -> ResponseT: ...

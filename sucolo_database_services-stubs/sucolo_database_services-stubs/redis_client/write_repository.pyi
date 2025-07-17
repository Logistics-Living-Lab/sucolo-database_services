import geopandas as gpd
from redis import Redis
from redis.typing import ResponseT as ResponseT


class RedisWriteRepository:
    redis_client: Redis
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

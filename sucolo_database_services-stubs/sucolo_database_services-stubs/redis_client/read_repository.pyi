from _typeshed import Incomplete
from redis import Redis as Redis

from sucolo_database_services.redis_client.consts import (
    HEX_SUFFIX as HEX_SUFFIX,
)
from sucolo_database_services.redis_client.consts import (
    POIS_SUFFIX as POIS_SUFFIX,
)
from sucolo_database_services.redis_client.utils import (
    check_if_keys_exist as check_if_keys_exist,
)

class RedisReadRepository:
    redis_client: Incomplete
    def __init__(self, redis_client: Redis) -> None: ...
    def get_hexagons(self, city: str, resolution: int) -> list[str]: ...
    def count_records_per_key(self, city: str) -> dict[str, int]: ...
    def find_nearest_pois_to_hex_centers(
        self,
        city: str,
        amenity: str,
        resolution: int,
        radius: int = 300,
        count: int | None = 1,
    ) -> dict[str, list[float]]: ...

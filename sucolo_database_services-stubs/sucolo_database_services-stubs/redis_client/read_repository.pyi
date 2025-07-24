from redis import Redis


class RedisReadRepository:
    redis_client: Redis
    def __init__(self, redis_client: Redis) -> None: ...
    def key_exists(self, key: str) -> bool: ...
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

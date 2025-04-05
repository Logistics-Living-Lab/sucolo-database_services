from redis import Redis


class RedisKeysManager:
    def __init__(
        self,
        redis_client: Redis,
    ) -> None:
        self.redis_client = redis_client

    def get_city_keys(self, city: str) -> list[str]:
        city_keys = list(
            map(lambda key: key.decode("utf-8"), self.redis_client.scan_iter())
        )
        city_keys = list(filter(lambda key: city in key, city_keys))
        return city_keys

    def delete_city_keys(self, city: str) -> None:
        city_keys = self.get_city_keys(city)
        if len(city_keys) == 0:
            print(f'Warning: no key with "{city}" in name found.')
            return

        for key in city_keys:
            self.redis_client.delete(key)

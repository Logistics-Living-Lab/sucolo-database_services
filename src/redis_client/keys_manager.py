from redis import Redis


class RedisKeysManager:
    def __init__(
        self,
        redis_client: Redis,
        city: str,
    ) -> None:
        self.redis_client = redis_client
        self.city = city

    def get_city_keys(self) -> list[str]:
        city_keys = list(
            map(lambda key: key.decode("utf-8"), self.redis_client.scan_iter())
        )
        city_keys = list(filter(lambda key: self.city in key, city_keys))
        return city_keys

    def delete_city_keys(
        self,
    ) -> None:
        city_keys = self.get_city_keys()
        if len(city_keys) == 0:
            print(f'Warning: no key with "{self.city}" in name found.')
            return

        for key in city_keys:
            self.redis_client.delete(key)

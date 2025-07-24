from redis import Redis as Redis


class RedisKeyNotFoundError(Exception):
    """Exception raised when a Redis key is not found."""
    pass


def check_if_keys_exist(client: Redis, keys: str | list[str]) -> None: ...

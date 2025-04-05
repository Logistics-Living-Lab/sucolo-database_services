from redis import Redis


def check_if_keys_exist(client: Redis, keys: str | list[str]) -> None:
    keys = [keys] if isinstance(keys, str) else keys
    not_found_keys = list(filter(lambda key: not client.exists(key), keys))
    if len(not_found_keys) > 0:
        raise ValueError(f"Keys {not_found_keys} not found.")

from redis import Redis

from sucolo_database_services.redis_client.keys_manager import (
    RedisKeysManager
)
from sucolo_database_services.redis_client.read_repository import (
    RedisReadRepository
)
from sucolo_database_services.redis_client.write_repository import (
    RedisWriteRepository
)


class RedisService:
    keys_manager: RedisKeysManager
    read: RedisReadRepository
    write: RedisWriteRepository
    def __init__(self, redis_client: Redis) -> None: ...
    def check_health(self) -> bool: ...

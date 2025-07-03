from _typeshed import Incomplete
from redis import Redis as Redis

from sucolo_database_services.redis_client.keys_manager import (
    RedisKeysManager as RedisKeysManager,
)
from sucolo_database_services.redis_client.read_repository import (
    RedisReadRepository as RedisReadRepository,
)
from sucolo_database_services.redis_client.write_repository import (
    RedisWriteRepository as RedisWriteRepository,
)

class RedisService:
    keys_manager: Incomplete
    read: Incomplete
    write: Incomplete
    def __init__(self, redis_client: Redis) -> None: ...
    def check_health(self) -> bool: ...

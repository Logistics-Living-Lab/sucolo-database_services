from redis import Redis

from sucolo_database_services.redis_client.keys_manager import RedisKeysManager
from sucolo_database_services.redis_client.read_repository import (
    RedisReadRepository,
)
from sucolo_database_services.redis_client.write_repository import (
    RedisWriteRepository,
)


class RedisService:
    def __init__(
        self,
        redis_client: Redis,
    ) -> None:
        self.redis_client = redis_client

        self.keys_manager = RedisKeysManager(
            redis_client=redis_client,
        )
        self.read = RedisReadRepository(
            redis_client=redis_client,
        )
        self.write = RedisWriteRepository(
            redis_client=redis_client,
        )

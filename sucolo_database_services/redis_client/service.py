from redis import ConnectionError, Redis

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
        self._redis_client = redis_client

        self.keys_manager = RedisKeysManager(
            redis_client=self._redis_client,
        )
        self.read = RedisReadRepository(
            redis_client=self._redis_client,
        )
        self.write = RedisWriteRepository(
            redis_client=self._redis_client,
        )

    def check_health(self) -> bool:
        """Check if Redis is reachable."""
        try:
            self._redis_client.ping()
            return True
        except ConnectionError:
            return False

from redis import Redis

from src.redis_module.client.keys_manager import RedisKeysManager
from src.redis_module.client.read_repository import RedisReadRepository
from src.redis_module.client.write_repository import RedisWriteRepository


class RedisService:
    def __init__(
        self,
        redis_client: Redis,
        city: str,
    ) -> None:
        self.redis_client = redis_client
        self.city = city

        self.keys_manager = RedisKeysManager(
            redis_client=redis_client,
            city=city,
        )
        self.read = RedisReadRepository(
            redis_client=redis_client,
            city=city,
        )
        self.write = RedisWriteRepository(
            redis_client=redis_client,
            city=city,
        )

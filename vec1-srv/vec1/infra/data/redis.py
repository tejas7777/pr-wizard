import redis


class RedisClientFactory:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self._client = redis.Redis(host=host, port=port, db=db)

    def get_client(self) -> redis.Redis:
        return self._client

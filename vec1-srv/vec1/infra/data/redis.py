import redis

class RedisClient:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self._client = redis.Redis(host=host, port=port, db=db)

    def set(self, key: str, value: str) -> None:
        self._client.set(key, value)

    def get(self, key: str) -> str | None:
        value = self._client.get(key)
        if value is not None:
            return value.decode('utf-8')
        return None
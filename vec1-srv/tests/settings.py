from vec1.config.settings import (
    Settings,
    GitHubSettings,
    LoggingSettings,
    QueueSettings,
    ChunkerSettings,
    JobSettings,
    StateSettings,
)


def get_test_settings() -> Settings:
    return Settings(
        github=GitHubSettings(
            token="test-token",
            owner="test-owner",
            repo="test-repo",
        ),
        logging=LoggingSettings(
            backend="console",
            name="vec1-test",
            logfire_token=None,
        ),
        queue=QueueSettings(max_size=100),
        chunker=ChunkerSettings(diff_max_tokens=2000),
        jobs=JobSettings(
            poll_interval=1.0,
            chunk_get_timeout=0.1,
            default_lookback_hours=24,
        ),
        state=StateSettings(base_dir="/tmp/vec1-test-state"),
    )

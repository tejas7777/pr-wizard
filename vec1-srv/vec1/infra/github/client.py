from github import Github


class GitHubClient:
    def __init__(self, token: str) -> None:
        self._client = Github(login_or_token=token)

    def get_repo(self, owner: str, name: str):
        return self._client.get_repo(f'{owner}/{name}')

    def close(self) -> None:
        try:
            self._client.close()
        except AttributeError:
            return

    def __enter__(self) -> 'GitHubClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.close()

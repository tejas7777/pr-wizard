# vec1

## GitHub configuration
- Create a GitHub personal access token with `repo` scope for private repositories or `public_repo` for public repositories.
- Export environment variables: `GITHUB_TOKEN` with the token value, `VEC1_REPO_OWNER` with the repository owner, and `VEC1_REPO_NAME` with the repository name.
- The token must allow read access to pull requests, pull request reviews, and repository contents.

## Setup with uv
- Install uv from https://docs.astral.sh/uv/.
- Create the environment and install deps: `uv sync`.
- Run the app: `uv run python main.py`.

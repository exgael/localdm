# Standard library
from pathlib import Path

# -----------------------------
# Content-addressed storage
# -----------------------------


def get_object_path(repo_path: Path, hash_val: str) -> Path:
    """
    Get path for content-addressed object.

    Uses first 2 characters of hash for directory sharding.

    Example:
        hash="abc123..." -> repo_path/objects/ab/c123...parquet
    """
    return repo_path / "objects" / hash_val[:2] / f"{hash_val[2:]}.parquet"


def init_repo(repo_path: Path) -> None:
    """Initialize repository structure."""
    repo_path.mkdir(parents=True, exist_ok=True)
    (repo_path / "objects").mkdir(exist_ok=True)


def get_metadata_db_path(repo_path: Path) -> Path:
    """Get path to metadata database."""
    return repo_path / "metadata.db"

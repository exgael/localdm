# Standard library
from pathlib import Path

# Local imports
from localdm.core.metadata import DatasetMetadata, load_metadata, resolve_ref_to_hash
from localdm.core.storage import get_metadata_db_path
from localdm.datasets.dataset import Dataset

# -----------------------------
# Dataset Repository
# -----------------------------


class DatasetRepository:
    """Handles dataset retrieval without creating circular dependencies."""

    def __init__(self, repo_path: Path) -> None:
        self.repo_path: Path = repo_path
        self.db_path: Path = get_metadata_db_path(repo_path)

    def get_metadata(self, ref: str) -> DatasetMetadata:
        """Load metadata by reference."""
        hash_val: str = resolve_ref_to_hash(self.db_path, ref)
        return load_metadata(self.db_path, hash_val)

    def get(self, ref: str) -> Dataset:
        """Get dataset by reference without circular dependency."""
        metadata: DatasetMetadata = self.get_metadata(ref)
        return Dataset(metadata=metadata, _data_cache=None)

    def get_many(self, refs: list[str]) -> list[Dataset]:
        """Get multiple datasets by reference."""
        return [self.get(ref) for ref in refs]

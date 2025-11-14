# Standard library
from pathlib import Path
from typing import TYPE_CHECKING

# Third-party
import polars as pl

# Local imports
from localdm.core.metadata import DatasetMetadata, load_metadata, resolve_ref_to_hash
from localdm.core.storage import get_metadata_db_path

if TYPE_CHECKING:
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

    def get(self, ref: str) -> "Dataset":
        """Get dataset by reference without circular dependency."""
        # Avoid circular import at runtime
        from localdm.datasets.dataset import Dataset

        metadata: DatasetMetadata = self.get_metadata(ref)
        data_cache: pl.DataFrame = pl.read_parquet(Path(metadata.data_path))

        return Dataset(
            metadata=metadata,
            _data_cache=data_cache,
        )

    def get_many(self, refs: list[str]) -> list["Dataset"]:
        """Get multiple datasets by reference."""
        return [self.get(ref) for ref in refs]

# Standard library
from pathlib import Path

# Third-party
import polars as pl

# Local imports
from localdm.core.models import DatasetMetadata
from localdm.core.storage import get_object_path

# -----------------------------
# Data Repository
# -----------------------------


class DataRepository:
    """Repository for dataset parquet file I/O operations."""

    def __init__(self, repo_path: Path) -> None:
        self.repo_path: Path = repo_path

    def save_data(self, data: pl.DataFrame, metadata: DatasetMetadata) -> None:
        """Save DataFrame to parquet file.

        Args:
            data: DataFrame to save
            metadata: Metadata containing hash for file path
        """
        hash_val: str = metadata.hash
        data_path: Path = get_object_path(self.repo_path, hash_val)
        data_path.parent.mkdir(parents=True, exist_ok=True)
        data.write_parquet(data_path)

    def load_data(self, hash_val: str) -> pl.LazyFrame:
        """Load DataFrame from parquet file.

        Args:
            hash_val: Dataset hash

        Returns:
            LazyFrame for lazy evaluation
        """
        data_path: Path = get_object_path(self.repo_path, hash_val)
        return pl.scan_parquet(data_path)

    def delete_data(self, hash_val: str) -> None:
        """Delete parquet file.

        Args:
            hash_val: Dataset hash
        """
        data_path: Path = get_object_path(self.repo_path, hash_val)
        if data_path.exists():
            data_path.unlink()

    def data_exists(self, hash_val: str) -> bool:
        """Check if parquet file exists.

        Args:
            hash_val: Dataset hash

        Returns:
            True if file exists, False otherwise
        """
        data_path: Path = get_object_path(self.repo_path, hash_val)
        return data_path.exists()


# Backward compatibility alias
DatasetRepository = DataRepository

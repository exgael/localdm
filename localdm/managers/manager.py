# Standard library
import os
from pathlib import Path

# Third-party
import polars as pl

# Local imports
from localdm.core.metadata import DatasetMetadata, init_database, save_metadata
from localdm.core.shared import (
    compute_hash,
    compute_stats,
    create_metadata,
    extract_schema,
    extract_transform_type,
)
from localdm.core.storage import get_metadata_db_path, get_object_path, init_repo
from localdm.datasets.dataset import Dataset
from localdm.repositories.dataset_repository import DatasetRepository

# -----------------------------
# Unified Data Manager
# -----------------------------


class DataManager:
    """Data manager using Polars engine with Pandas interop."""

    def __init__(self, repo_path: str | Path | None = None) -> None:
        if repo_path is None:
            env_path: str | None = os.getenv("LOCALDM_REPO")
            if env_path:
                self.repo_path = Path(env_path).expanduser().resolve()
            else:
                self.repo_path = Path.cwd() / ".localdm"
        else:
            self.repo_path = Path(repo_path).expanduser().resolve()

        init_repo(self.repo_path)
        init_database(get_metadata_db_path(self.repo_path))

        # Single repository
        self.repository = DatasetRepository(self.repo_path)

    # -----------------------------
    # Public methods
    # -----------------------------

    def create_dataset(
        self,
        name: str,
        data: pl.DataFrame,
        tag: str | None = None,
        parents: list[Dataset | str] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> Dataset:
        """Create new dataset from computed data."""
        parent_refs: list[str] = []
        if parents:
            for p in parents:
                if isinstance(p, Dataset):
                    parent_refs.append(p.full_ref)
                else:
                    parent_refs.append(p)

        transform_type: str | None = extract_transform_type(metadata)

        dataset_metadata: DatasetMetadata = self._save_dataset(
            data=data,
            name=name,
            tag=tag,
            parent_refs=parent_refs,
            transform_type=transform_type,
            transform_metadata=metadata,
        )

        return Dataset(metadata=dataset_metadata, _data_cache=None)

    def get(self, ref: str) -> Dataset:
        """Get dataset by reference."""
        return self.repository.get(ref)

    def load_metadata(self, ref: str) -> DatasetMetadata:
        """Load metadata only (no data)."""
        return self.repository.get_metadata(ref)

    # -----------------------------
    # Private shared logic
    # -----------------------------

    def _save_dataset(
        self,
        data: pl.DataFrame,
        name: str,
        tag: str | None,
        parent_refs: list[str],
        transform_type: str | None,
        transform_metadata: dict[str, object] | None,
    ) -> DatasetMetadata:
        """Save dataset to storage."""
        hash_val: str = compute_hash(data)
        schema: dict[str, str] = extract_schema(data)
        stats: dict[str, object] = compute_stats(data)

        data_path: Path = get_object_path(self.repo_path, hash_val)
        data_path.parent.mkdir(parents=True, exist_ok=True)
        data.write_parquet(data_path)

        metadata: DatasetMetadata = create_metadata(
            hash_val=hash_val,
            name=name,
            tags=[tag] if tag else [],
            parent_refs=parent_refs,
            transform_type=transform_type,
            transform_metadata=transform_metadata,
            schema=schema,
            stats=stats,
            data_path=str(data_path),
        )

        save_metadata(get_metadata_db_path(self.repo_path), metadata)

        return metadata

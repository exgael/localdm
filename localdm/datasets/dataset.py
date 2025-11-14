# Standard library
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

# Third-party
import polars as pl

# Local imports
from localdm.core.metadata import DatasetMetadata
from localdm.core.shared import get_parent_ref_by_name

if TYPE_CHECKING:
    from localdm.managers.manager import DataManager
    from localdm.repositories.dataset_repository import DatasetRepository

# -----------------------------
# Dataset
# -----------------------------


@dataclass(frozen=True)
class Dataset:
    """Immutable dataset using Polars for storage."""

    metadata: DatasetMetadata
    _data_cache: pl.DataFrame | None = None

    @property
    def hash(self) -> str:
        return self.metadata.hash

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def tags(self) -> list[str]:
        return self.metadata.tags

    @property
    def ref(self) -> str:
        return self.metadata.ref

    @property
    def full_ref(self) -> str:
        return self.metadata.full_ref

    @property
    def df(self) -> pl.LazyFrame:
        """Get LazyFrame - data not loaded until collect() is called."""
        return pl.scan_parquet(Path(self.metadata.data_path))

    def get_parents(self, repository: "DatasetRepository") -> list["Dataset"]:
        """Load all parent Dataset objects via repository."""
        return [repository.get(ref) for ref in self.metadata.parent_refs]

    def get_parent(self, name: str, repository: "DatasetRepository") -> "Dataset":
        """Get specific parent by name via repository."""
        ref: str = get_parent_ref_by_name(self.metadata.parent_refs, name)
        return repository.get(ref)

    def derive(
        self,
        data: pl.DataFrame,
        manager: "DataManager",
        name: str | None = None,
        tag: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> "Dataset":
        """Create derived dataset via manager."""
        return manager.create_dataset(
            name=name or self.name,
            tag=tag,
            data=data,
            parents=[self],
            metadata=metadata,
        )

    def __repr__(self) -> str:
        tags: str = f"[{','.join(self.tags)}]" if self.tags else ""
        return f"Dataset({self.name}{tags}, hash={self.hash[:7]}...)"

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

    def info(self) -> None:
        """Display dataset information using rich formatting."""
        from rich.console import Console
        from rich.table import Table

        console: Console = Console()

        # Overview table
        info_table: Table = Table(title=f"Dataset: {self.ref}", show_header=False)
        info_table.add_column("Property", style="cyan", width=20)
        info_table.add_column("Value", style="white")

        info_table.add_row("Name", self.name)
        info_table.add_row("Hash", self.hash[:12])
        info_table.add_row("Created", self.metadata.created_at)
        info_table.add_row("Author", self.metadata.author)
        info_table.add_row("Tags", ", ".join(self.tags) if self.tags else "-")

        if self.metadata.stats:
            info_table.add_row("Rows", str(self.metadata.stats.get("row_count", "N/A")))
            info_table.add_row(
                "Columns", str(self.metadata.stats.get("column_count", "N/A"))
            )

        console.print(info_table)

        # Schema table
        if self.metadata.schema:
            schema_table: Table = Table(title="Schema", show_header=True)
            schema_table.add_column("Column", style="green")
            schema_table.add_column("Type", style="yellow")

            for col, dtype in self.metadata.schema.items():
                schema_table.add_row(col, dtype)

            console.print(schema_table)

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
        created: str = self.metadata.created_at.split("T")[0]
        author: str = self.metadata.author
        h: str = self.hash[:7]
        return (
            f"Dataset({self.name}{tags}, hash={h}, author={author}, created={created})"
        )

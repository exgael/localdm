# Standard library
from dataclasses import dataclass
from typing import TypedDict

# -----------------------------
# TypedDict Definitions
# -----------------------------


class ColumnStats(TypedDict):
    """Statistics for a single column."""

    null_count: int
    null_percentage: float
    unique_count: int


class DatasetStats(TypedDict):
    """Complete dataset statistics."""

    row_count: int
    column_count: int
    column_stats: dict[str, ColumnStats]


# -----------------------------
# Domain Models
# -----------------------------


@dataclass(frozen=True)
class DatasetMetadata:
    """Immutable dataset metadata."""

    id: str
    hash: str
    name: str
    tags: list[str]
    created_at: str
    updated_at: str
    author: str
    parent_refs: list[str]
    description: str | None
    schema: dict[str, str] | None
    stats: DatasetStats | None
    data_path: str

    @property
    def ref(self) -> str:
        """Get preferred reference (first tag or hash)."""
        if self.tags:
            return f"{self.name}:{self.tags[0]}"
        return f"{self.name}@{self.hash[:7]}"

    @property
    def full_ref(self) -> str:
        """Get full reference with hash."""
        return f"{self.name}@{self.hash}"

    def __repr__(self) -> str:
        tags = f"[{','.join(self.tags)}]" if self.tags else "[]"
        return f"Metadata({self.name}{tags}, id={self.id}, desc={self.description})"

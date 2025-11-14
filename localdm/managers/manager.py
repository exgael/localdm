# Standard library
import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

# Third-party
import polars as pl

# Local imports
from localdm.core.metadata import (
    DatasetMetadata,
    init_database,
    load_metadata,
    save_metadata,
)
from localdm.core.shared import (
    compute_hash,
    compute_stats,
    create_metadata,
    extract_schema,
    extract_transform_type,
    get_current_username,
)
from localdm.core.storage import get_metadata_db_path, get_object_path, init_repo
from localdm.datasets.dataset import Dataset
from localdm.repositories.dataset_repository import DatasetRepository

if TYPE_CHECKING:
    from rich.tree import Tree

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
        author: str | None = None,
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

        # Auto-detect author if not provided
        if author is None:
            author = get_current_username()

        dataset_metadata: DatasetMetadata = self._save_dataset(
            data=data,
            name=name,
            tag=tag,
            parent_refs=parent_refs,
            transform_type=transform_type,
            transform_metadata=metadata,
            author=author,
        )

        return Dataset(metadata=dataset_metadata, _data_cache=None)

    def get(self, ref: str) -> Dataset:
        """Get dataset by reference."""
        return self.repository.get(ref)

    def load_metadata(self, ref: str) -> DatasetMetadata:
        """Load metadata only (no data)."""
        return self.repository.get_metadata(ref)

    def add_tag(self, ref: str, tag: str) -> None:
        """Add a tag to an existing dataset."""
        from localdm.core.metadata import resolve_ref_to_hash

        db_path: Path = get_metadata_db_path(self.repo_path)

        # Resolve ref to hash
        hash_val: str = resolve_ref_to_hash(db_path, ref)

        # Get dataset name
        conn: sqlite3.Connection = sqlite3.connect(db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                "SELECT name FROM datasets WHERE hash = ?", (hash_val,)
            )
            row: object = cursor.fetchone()
            if not row:
                msg = f"Dataset with hash '{hash_val}' not found"
                raise KeyError(msg)

            name: str = row[0]  # type: ignore[index]

            # Check if tag already exists
            existing: sqlite3.Cursor = conn.execute(
                "SELECT 1 FROM tags WHERE name = ? AND tag = ?", (name, tag)
            )
            if existing.fetchone():
                return  # Tag already exists, nothing to do

            # Insert new tag
            from datetime import UTC, datetime

            conn.execute(
                "INSERT INTO tags (name, tag, hash, created_at) VALUES (?, ?, ?, ?)",
                (name, tag, hash_val, datetime.now(UTC).isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    def remove_tag(self, ref: str, tag: str) -> None:
        """Remove a tag from a dataset."""
        from localdm.core.metadata import resolve_ref_to_hash

        db_path: Path = get_metadata_db_path(self.repo_path)

        # Resolve ref to hash
        hash_val: str = resolve_ref_to_hash(db_path, ref)

        # Get dataset name
        conn: sqlite3.Connection = sqlite3.connect(db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                "SELECT name FROM datasets WHERE hash = ?", (hash_val,)
            )
            row: object = cursor.fetchone()
            if not row:
                msg = f"Dataset with hash '{hash_val}' not found"
                raise KeyError(msg)

            name: str = row[0]  # type: ignore[index]

            # Check if tag exists
            tag_cursor: sqlite3.Cursor = conn.execute(
                "SELECT 1 FROM tags WHERE name = ? AND tag = ?", (name, tag)
            )
            if not tag_cursor.fetchone():
                msg = f"Tag '{tag}' not found for dataset '{name}'"
                raise KeyError(msg)

            # Delete tag
            conn.execute("DELETE FROM tags WHERE name = ? AND tag = ?", (name, tag))
            conn.commit()
        finally:
            conn.close()

    def list_tags(self, dataset_name: str) -> list[tuple[str, str]]:
        """List all tags for a dataset name with timestamps."""
        conn: sqlite3.Connection = sqlite3.connect(get_metadata_db_path(self.repo_path))
        try:
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT tag, created_at
                FROM tags
                WHERE name = ?
                ORDER BY created_at DESC
                """,
                (dataset_name,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            conn.close()

    def list_datasets(
        self,
        name_filter: str | None = None,
        limit: int | None = None,
    ) -> list[DatasetMetadata]:
        """List all datasets with optional filtering."""
        conn: sqlite3.Connection = sqlite3.connect(get_metadata_db_path(self.repo_path))
        try:
            query: str = "SELECT DISTINCT hash FROM datasets WHERE 1=1"
            params: list[object] = []

            if name_filter:
                query += " AND name LIKE ?"
                params.append(f"%{name_filter}%")

            query += " ORDER BY created_at DESC"

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            cursor: sqlite3.Cursor = conn.execute(query, params)
            hashes: list[str] = [row[0] for row in cursor.fetchall()]

            return [
                load_metadata(get_metadata_db_path(self.repo_path), h) for h in hashes
            ]
        finally:
            conn.close()

    def show_datasets(self, name_filter: str | None = None) -> None:
        """Display all datasets in a rich table."""
        from rich.console import Console
        from rich.table import Table

        console: Console = Console()
        datasets: list[DatasetMetadata] = self.list_datasets(name_filter=name_filter)

        table: Table = Table(
            title="Datasets", show_header=True, header_style="bold magenta"
        )
        table.add_column("Name", style="cyan")
        table.add_column("Tags", style="green")
        table.add_column("Hash", style="yellow")
        table.add_column("Created", style="blue")
        table.add_column("Author", style="magenta")
        table.add_column("Rows", justify="right")

        for meta in datasets:
            table.add_row(
                meta.name,
                ", ".join(meta.tags) if meta.tags else "-",
                meta.hash[:7],
                meta.created_at.split("T")[0],
                meta.author,
                str(meta.stats.get("row_count", "?")) if meta.stats else "?",
            )

        console.print(table)

    def describe(self, ref: str) -> None:
        """Display detailed information about a dataset."""
        from rich.console import Console
        from rich.panel import Panel

        console: Console = Console()
        meta: DatasetMetadata = self.load_metadata(ref)

        sections: list[str] = self._build_describe_sections(meta)

        panel: Panel = Panel(
            "\n".join(sections),
            title=f"Dataset: {meta.ref}",
            border_style="bright_blue",
        )
        console.print(panel)

    def visualize_lineage(self, ref: str, max_depth: int = 5) -> None:
        """Display lineage tree for a dataset."""
        from rich.console import Console
        from rich.tree import Tree

        console: Console = Console()
        meta: DatasetMetadata = self.load_metadata(ref)

        tree: Tree = Tree(f"[bold cyan]{meta.ref}[/] ({meta.created_at.split('T')[0]})")

        self._add_parents_to_tree(tree, meta, 0, max_depth)

        console.print(tree)

    # -----------------------------
    # Private shared logic
    # -----------------------------

    def _build_describe_sections(self, meta: DatasetMetadata) -> list[str]:
        """Build description sections for a dataset."""
        import json

        sections: list[str] = []

        # Basic info
        sections.append(f"[bold cyan]Name:[/] {meta.name}")
        sections.append(f"[bold cyan]Hash:[/] {meta.hash[:12]}")
        sections.append(f"[bold cyan]Created:[/] {meta.created_at}")
        sections.append(f"[bold cyan]Author:[/] {meta.author}")

        if meta.tags:
            sections.append(f"[bold cyan]Tags:[/] {', '.join(meta.tags)}")

        # Statistics
        if meta.stats:
            sections.append("")
            sections.append("[bold yellow]Statistics:[/]")
            sections.append(f"  Rows: {meta.stats.get('row_count', 'N/A')}")
            sections.append(f"  Columns: {meta.stats.get('column_count', 'N/A')}")

        # Schema
        if meta.schema:
            sections.append("")
            sections.append("[bold green]Schema:[/]")
            for col, dtype in meta.schema.items():
                sections.append(f"  {col}: {dtype}")

        # Parents
        if meta.parent_refs:
            sections.append("")
            sections.append("[bold magenta]Parents:[/]")
            sections.extend(f"  - {parent}" for parent in meta.parent_refs)

        # Transform info
        if meta.transform_type:
            sections.append("")
            sections.append(f"[bold blue]Transform:[/] {meta.transform_type}")
            if meta.transform_metadata:
                sections.append(json.dumps(meta.transform_metadata, indent=2))

        return sections

    def _add_parents_to_tree(
        self,
        node: "Tree",
        dataset_meta: DatasetMetadata,
        depth: int,
        max_depth: int,
    ) -> None:
        """Recursively add parents to lineage tree."""

        if depth >= max_depth or not dataset_meta.parent_refs:
            return

        for parent_ref in dataset_meta.parent_refs:
            parent_meta: DatasetMetadata = self.load_metadata(parent_ref)
            parent_node: Tree = node.add(
                f"[green]{parent_meta.ref}[/] ({parent_meta.created_at.split('T')[0]})"
            )
            self._add_parents_to_tree(parent_node, parent_meta, depth + 1, max_depth)

    def _save_dataset(
        self,
        data: pl.DataFrame,
        name: str,
        tag: str | None,
        parent_refs: list[str],
        transform_type: str | None,
        transform_metadata: dict[str, object] | None,
        author: str,
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
            author=author,
            transform_type=transform_type,
            transform_metadata=transform_metadata,
            schema=schema,
            stats=stats,
            data_path=str(data_path),
        )

        save_metadata(get_metadata_db_path(self.repo_path), metadata)

        return metadata

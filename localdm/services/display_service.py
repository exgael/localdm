# Standard library

# Third-party
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

# Local imports
from localdm.core.models import ColumnStats, DatasetMetadata
from localdm.repositories.metadata_repository import MetadataRepository
from localdm.services.lineage_service import LineageService

# -----------------------------
# Constants
# -----------------------------

NULL_PCT_HIGH_THRESHOLD = 50
NULL_PCT_MEDIUM_THRESHOLD = 10

# -----------------------------
# Display Service
# -----------------------------


class DisplayService:
    """Service for Rich console formatting and visualization."""

    def __init__(
        self,
        metadata_repo: MetadataRepository,
        lineage_service: LineageService,
    ) -> None:
        self.metadata_repo: MetadataRepository = metadata_repo
        self.lineage_service: LineageService = lineage_service
        self.console = Console()

    # -----------------------------
    # Display Operations
    # -----------------------------

    def show_tree(self) -> None:
        """Render a full dataset lineage tree."""
        root: Tree = Tree("[bold cyan]Datasets[/]")

        # Load all metadata
        all_meta: dict[str, DatasetMetadata] = {
            meta.ref: meta for meta in self.metadata_repo.list_datasets()
        }

        # parent → children
        children: dict[str, list[str]] = {}
        for meta in all_meta.values():
            for parent in meta.parent_refs or []:
                children.setdefault(parent, []).append(meta.ref)

        # roots = datasets without parents
        root_refs: list[str] = [
            ref for ref, meta in all_meta.items() if not meta.parent_refs
        ]

        def fmt(meta: DatasetMetadata) -> str:
            tags: str = f"[{', '.join(meta.tags)}]" if meta.tags else ""
            date: str = meta.created_at.split("T")[0]
            return f"[green]{meta.name}[/]  {tags}   id={meta.id}   {date}"

        def add_subtree(node: Tree, ref: str) -> None:
            meta: DatasetMetadata = all_meta[ref]
            sub: Tree = node.add(fmt(meta))
            for child_ref in sorted(children.get(ref, [])):
                add_subtree(sub, child_ref)

        for ref in sorted(root_refs):
            add_subtree(root, ref)

        self.console.print(root)

    def show_dataset_info(self, ref: str) -> None:
        """Display comprehensive information about a dataset.

        Args:
            ref: Dataset reference
        """
        dataset_id: str = self.metadata_repo.resolve_ref_to_id(ref)
        metadata: DatasetMetadata = self.metadata_repo.load(dataset_id)
        lineage_lines: list[str] = []

        # Get lineage display if dataset has parents
        if metadata.parent_refs:
            lineage_lines = self.lineage_service.build_lineage_display(ref)

        # Build and display panel
        panel: Panel = self._format_metadata_panel(metadata, lineage_lines)
        self.console.print(panel)

    def show_datasets_table(self, name_filter: str | None = None) -> None:
        """Display all datasets in a rich table.

        Args:
            name_filter: Optional name pattern to filter by
        """
        datasets: list[DatasetMetadata] = self.metadata_repo.list_datasets(
            name_filter=name_filter
        )

        table: Table = Table(
            title="Datasets", show_header=True, header_style="bold magenta"
        )
        table.add_column("Name", style="cyan")
        table.add_column("Tags", style="green")
        table.add_column("Hash", style="yellow")
        table.add_column("Created", style="blue")
        table.add_column("Updated", style="blue")
        table.add_column("Author", style="magenta")
        table.add_column("Rows", justify="right", style="white")
        table.add_column("Cols", justify="right", style="white")

        for meta in datasets:
            if meta.stats:
                rows: int = meta.stats["row_count"]
                cols: int = meta.stats["column_count"]
                rows_str: str = f"{rows:,}"
                cols_str = str(cols)
            else:
                rows_str = "?"
                cols_str = "?"

            table.add_row(
                meta.name,
                ", ".join(meta.tags) if meta.tags else "-",
                meta.hash[:7],
                meta.created_at.split("T")[0],
                meta.updated_at.split("T")[0],
                meta.author,
                rows_str,
                cols_str,
            )

        self.console.print(table)

    def visualize_lineage_tree(self, ref: str, max_depth: int = 5) -> None:
        """Display lineage tree for a dataset.

        Args:
            ref: Dataset reference
            max_depth: Maximum depth to traverse
        """
        dataset_id: str = self.metadata_repo.resolve_ref_to_id(ref)
        meta: DatasetMetadata = self.metadata_repo.load(dataset_id)

        tree: Tree = Tree(f"[bold cyan]{meta.ref}[/] ({meta.created_at.split('T')[0]})")

        # Build parent tree recursively
        self._build_parent_tree(tree, meta, 0, max_depth)

        self.console.print(tree)

    # -----------------------------
    # Formatting Helpers
    # -----------------------------

    def _format_metadata_panel(
        self, metadata: DatasetMetadata, lineage_lines: list[str]
    ) -> Panel:
        """Format metadata as Rich panel.

        Args:
            metadata: Dataset metadata
            lineage_lines: Pre-formatted lineage hierarchy lines

        Returns:
            Rich Panel with formatted metadata
        """
        sections: list[str] = []

        # Basic info
        sections.append(f"[bold cyan]ID:[/] {metadata.id}")
        sections.append(f"[bold cyan]Name:[/] {metadata.name}")
        sections.append(f"[bold cyan]Hash:[/] {metadata.hash[:12]}")
        sections.append(f"[bold cyan]Created:[/] {metadata.created_at}")
        sections.append(f"[bold cyan]Updated:[/] {metadata.updated_at}")
        sections.append(f"[bold cyan]Author:[/] {metadata.author}")

        if metadata.tags:
            sections.append(f"[bold cyan]Tags:[/] {', '.join(metadata.tags)}")

        # Statistics
        if metadata.stats:
            sections.append("")
            sections.append("[bold yellow]Statistics:[/]")
            row_count: int = metadata.stats["row_count"]
            sections.append(f"  Rows: {row_count:,}")
            sections.append(f"  Columns: {metadata.stats['column_count']}")

            # Column statistics
            column_stats: dict[str, ColumnStats] = metadata.stats["column_stats"]
            if column_stats:
                sections.append("")
                sections.append("[bold yellow]Column Details:[/]")
                for col_name, col_stat in column_stats.items():
                    null_pct: float = col_stat["null_percentage"]
                    unique: int = col_stat["unique_count"]
                    dtype: str = (
                        metadata.schema.get(col_name, "?") if metadata.schema else "?"
                    )

                    # Color code by null percentage
                    if null_pct > NULL_PCT_HIGH_THRESHOLD:
                        color = "red"
                    elif null_pct > NULL_PCT_MEDIUM_THRESHOLD:
                        color = "yellow"
                    else:
                        color = "green"

                    unique_str = f"{unique:,}"
                    sections.append(
                        f"  {col_name} ({dtype}): {unique_str} unique, "
                        f"[{color}]{null_pct:.5f}% null[/]"
                    )

        # Schema (more compact if we already showed column details)
        if metadata.schema and not (metadata.stats and metadata.stats["column_stats"]):
            sections.append("")
            sections.append(f"[bold green]Schema:[/] {len(metadata.schema)} columns")
            for col, dtype in metadata.schema.items():
                sections.append(f"  {col}: {dtype}")

        # Description
        if metadata.description:
            sections.append("")
            sections.append("[bold blue]Description:[/]")
            sections.append(f"  {metadata.description}")

        # Lineage hierarchy
        if lineage_lines:
            sections.append("")
            sections.append("[bold magenta]Lineage:[/]")
            sections.extend(lineage_lines)

        return Panel(
            "\n".join(sections),
            title=f"Dataset: {metadata.ref}",
            border_style="bright_blue",
        )

    def _build_parent_tree(
        self,
        node: Tree,
        dataset_meta: DatasetMetadata,
        depth: int,
        max_depth: int,
    ) -> None:
        """Recursively build parent lineage tree.

        Args:
            node: Rich Tree node to add parents to
            dataset_meta: Metadata of current dataset
            depth: Current recursion depth
            max_depth: Maximum depth to traverse
        """
        if depth >= max_depth or not dataset_meta.parent_refs:
            return

        for parent_id in dataset_meta.parent_refs:
            parent_meta: DatasetMetadata = self.metadata_repo.load(parent_id)
            parent_node: Tree = node.add(
                f"[green]{parent_meta.ref}[/] ({parent_meta.created_at.split('T')[0]})"
            )
            self._build_parent_tree(parent_node, parent_meta, depth + 1, max_depth)

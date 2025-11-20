# Standard library
import os
from pathlib import Path

# Third-party
import polars as pl

# Local imports
from localdm.core.models import DatasetMetadata
from localdm.core.storage import get_metadata_db_path, init_repo
from localdm.repositories.data_repository import DataRepository
from localdm.repositories.metadata_repository import MetadataRepository
from localdm.services.dataset_service import DatasetService
from localdm.services.display_service import DisplayService
from localdm.services.lineage_service import LineageService

# -----------------------------
# Unified Data Manager
# -----------------------------


class DataManager:
    """Data manager - single API surface for dataset operations.
    """

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
        db_path: Path = get_metadata_db_path(self.repo_path)

        # Initialize repositories
        self._data_repo = DataRepository(self.repo_path)
        self._metadata_repo = MetadataRepository(db_path)
        self._metadata_repo.init_database()

        # Initialize services
        self._dataset_service = DatasetService(
            data_repo=self._data_repo,
            metadata_repo=self._metadata_repo,
        )
        self._lineage_service = LineageService(self._metadata_repo)
        self._display_service = DisplayService(
            metadata_repo=self._metadata_repo,
            lineage_service=self._lineage_service,
        )

    # -----------------------------
    # Data Access
    # -----------------------------

    def get(self, dataset_id: str) -> pl.LazyFrame:
        """Get dataset data as LazyFrame by ID.

        Args:
            dataset_id: Dataset UUID

        Returns:
            Polars LazyFrame for lazy evaluation

        Examples:
            >>> meta = dm.list()[0]
            >>> df = dm.get(meta.id)
        """
        metadata: DatasetMetadata = self._metadata_repo.load(dataset_id)
        return self._data_repo.load_data(metadata.hash)
    
    def list_datasets(        
        self,
        name_filter: str | None = None,
        tag_filter: str | None = None,
    ) -> list[DatasetMetadata]:
        """Get all datasets with optional filtering.

        Args:
            name_filter: Optional name pattern to filter by
            tag_filter: Optional tag to filter by (exact match)

        Returns:
            List of DatasetMetadata objects
        """
        return self._metadata_repo.list_datasets(
            name_filter=name_filter,
            tag_filter=tag_filter,
        )

    # -----------------------------
    # Dataset Creation
    # -----------------------------

    def create_dataset(
        self,
        name: str,
        data: pl.DataFrame,
        tag: str | None = None,
        parents: list[str] | None = None,
        description: str | None = None,
        author: str | None = None,
    ) -> str:
        """Create new dataset from computed data.

        Args:
            name: Dataset name
            data: DataFrame to save
            tag: Optional tag name
            parents: Optional list of parent references (strings)
            description: Optional description (can be detailed, multiline text)
            author: Optional author (defaults to current username)

        Returns:
            Reference string to created dataset
        """
        dataset_metadata: DatasetMetadata = self._dataset_service.create_dataset(
            name=name,
            data=data,
            tag=tag,
            parent_refs=parents,
            description=description,
            author=author,
        )
        return dataset_metadata.ref

    def derive_dataset(
        self,
        source_ref: str,
        data: pl.DataFrame,
        name: str | None = None,
        tag: str | None = None,
        description: str | None = None,
    ) -> str:
        """Create new dataset derived from source dataset.

        Args:
            source_ref: Source dataset reference
            data: New data for derived dataset
            name: Optional new name (defaults to source name)
            tag: Optional tag for derived dataset
            description: Optional description (can be detailed, multiline text)

        Returns:
            Reference string to derived dataset
        """
        dataset_metadata: DatasetMetadata = self._dataset_service.derive_dataset(
            source_ref=source_ref,
            data=data,
            name=name,
            tag=tag,
            description=description,
        )
        return dataset_metadata.ref

    def update_dataset(
        self,
        dataset_id: str,
        data: pl.DataFrame,
        description: str | None = None,
    ) -> str:
        """Update dataset with new data.

        Replaces data and hash but keeps same ID, tags, and lineage.

        Args:
            dataset_id: Dataset UUID
            data: New DataFrame
            description: Optional new description (or keep existing)

        Returns:
            Dataset ID

        Example:
            # Load, modify, update
            meta = dm.list()[0]
            df = dm.get(meta.id).collect()
            df = df.with_columns(pl.col("age").cast(pl.Float64))
            dm.update_dataset(meta.id, df)
        """
        updated_metadata: DatasetMetadata = self._dataset_service.update_dataset(
            dataset_id=dataset_id,
            data=data,
            description=description,
        )
        return updated_metadata.id

    def delete(self, dataset_id: str, *, force: bool = False) -> None:
        """Delete dataset with safety checks.

        Args:
            dataset_id: Dataset UUID
            force: Skip confirmation warnings if True (keyword-only)
        """
        from rich.console import Console

        # Load metadata
        metadata: DatasetMetadata = self._metadata_repo.load(dataset_id)

        # Check for children
        children: list[DatasetMetadata] = self._metadata_repo.get_children(dataset_id)

        if children and not force:
            console = Console()
            child_refs: list[str] = [c.ref for c in children]
            console.print(
                f"[yellow]Warning:[/] Dataset '{metadata.ref}' has "
                f"{len(children)} child dataset(s):\n"
                f"  {', '.join(child_refs)}\n"
                f"Deleting will leave these with missing parent reference.\n"
                f"Use force=True to delete anyway."
            )
            return

        # Delete data and metadata
        self._data_repo.delete_data(metadata.hash)
        self._metadata_repo.delete_metadata(dataset_id)

    # -----------------------------
    # Tags
    # -----------------------------

    def add_tag(self, dataset_id: str, tag: str) -> None:
        """Add a tag to an existing dataset.

        Args:
            dataset_id: Dataset UUID
            tag: Tag name to add
        """
        self._metadata_repo.add_tag(dataset_id, tag)

    def remove_tag(self, dataset_id: str, tag: str) -> None:
        """Remove a tag from a dataset.

        Args:
            dataset_id: Dataset UUID
            tag: Tag name to remove
        """
        self._metadata_repo.remove_tag(dataset_id, tag)

    def list_tags(self, dataset_id: str) -> list[tuple[str, str]]:
        """List all tags for a dataset with timestamps.

        Args:
            dataset_id: Dataset UUID

        Returns:
            List of (tag, created_at) tuples
        """
        return self._metadata_repo.list_tags(dataset_id)

    # -----------------------------
    # Display Operations
    # -----------------------------

    def show(self, dataset_id: str) -> None:
        """Display comprehensive information about a dataset.

        Args:
            dataset_id: Dataset UUID
        """
        self._display_service.show_dataset_info(dataset_id)

    def show_all(self, name_filter: str | None = None) -> None:
        """Display all datasets in a rich table.

        Args:
            name_filter: Optional name pattern to filter by
        """
        self._display_service.show_datasets_table(name_filter=name_filter)

    def visualize_lineage(self, dataset_id: str, max_depth: int = 5) -> None:
        """Display lineage tree for a dataset.

        Args:
            dataset_id: Dataset UUID
            max_depth: Maximum depth to traverse
        """
        self._display_service.visualize_lineage_tree(dataset_id, max_depth)


# Local imports
from typing import TYPE_CHECKING

from localdm.repositories.metadata_repository import MetadataRepository

if TYPE_CHECKING:
    from localdm.core.models import DatasetMetadata

# -----------------------------
# Lineage Service
# -----------------------------


class LineageService:
    """Service for lineage traversal and hierarchy algorithms."""

    def __init__(self, metadata_repo: MetadataRepository) -> None:
        self.metadata_repo: MetadataRepository = metadata_repo

    # -----------------------------
    # Lineage Traversal
    # -----------------------------

    def find_root_datasets(self, ref: str) -> set[str]:
        """Find all root datasets by traversing parent ancestry.

        Args:
            ref: Dataset reference

        Returns:
            Set of root dataset references (datasets with no parents)
        """
        roots: set[str] = set()
        visited: set[str] = set()

        def traverse_to_roots(current_id: str) -> None:
            if current_id in visited:
                return
            visited.add(current_id)

            try:
                metadata: DatasetMetadata = self.metadata_repo.load(current_id)
                if not metadata.parent_refs:
                    roots.add(metadata.ref)
                else:
                    for parent_id in metadata.parent_refs:
                        traverse_to_roots(parent_id)
            except (KeyError, FileNotFoundError):
                roots.add(current_id)

        # Start traversal from given dataset's parents
        dataset_id: str = self.metadata_repo.resolve_ref_to_id(ref)
        metadata: DatasetMetadata = self.metadata_repo.load(dataset_id)
        for parent_id in metadata.parent_refs:
            traverse_to_roots(parent_id)

        return roots

    def get_parent_by_name(self, ref: str, parent_name: str) -> str:
        """Get specific parent reference by name.

        Args:
            ref: Dataset reference
            parent_name: Name of parent to retrieve

        Returns:
            Parent reference string

        Raises:
            ValueError: If no parent with given name found
        """
        dataset_id: str = self.metadata_repo.resolve_ref_to_id(ref)
        metadata: DatasetMetadata = self.metadata_repo.load(dataset_id)

        for parent_id in metadata.parent_refs:
            try:
                parent_meta: DatasetMetadata = self.metadata_repo.load(parent_id)
                if parent_meta.name == parent_name:
                    return parent_meta.ref
            except (KeyError, FileNotFoundError):
                continue

        msg = f"No parent with name '{parent_name}' found for dataset '{ref}'"
        raise ValueError(msg)

    # -----------------------------
    # Hierarchy Building
    # -----------------------------

    def build_lineage_display(self, ref: str) -> list[str]:
        """Build lineage display lines showing roots → parents → current.

        Shows hierarchy from root datasets down to current, with:
        - Root datasets in white
        - Immediate parents in white
        - Current dataset in bold green

        Args:
            ref: Dataset reference

        Returns:
            List of formatted display lines
        """
        lines: list[str] = []
        dataset_id: str = self.metadata_repo.resolve_ref_to_id(ref)
        metadata: DatasetMetadata = self.metadata_repo.load(dataset_id)
        roots: set[str] = self.find_root_datasets(ref)

        # Display roots
        for root_ref in sorted(roots):
            try:
                root_id: str = self.metadata_repo.resolve_ref_to_id(root_ref)
                root_meta: DatasetMetadata = self.metadata_repo.load(root_id)
                lines.append(f"  [white]{root_meta.ref}[/] (root)")
            except (KeyError, FileNotFoundError):
                lines.append(f"  [white]{root_ref}[/] (root, not found)")

        # Display immediate parents (if different from roots)
        # parent_refs are now IDs, need to convert to refs for display
        immediate_parent_metas: list[DatasetMetadata] = []
        for parent_id in metadata.parent_refs:
            try:
                parent_meta: DatasetMetadata = self.metadata_repo.load(parent_id)
                if parent_meta.ref not in roots:
                    immediate_parent_metas.append(parent_meta)
            except (KeyError, FileNotFoundError):
                continue

        if immediate_parent_metas:
            if roots:
                lines.append("    [dim]↓[/]")
            lines.extend(
                f"  [white]{parent_meta.ref}[/]"
                for parent_meta in immediate_parent_metas
            )

        # Display current dataset
        if metadata.parent_refs:
            lines.append("    [dim]↓[/]")
        lines.append(f"  [bold green]{metadata.ref}[/] (current)")

        return lines

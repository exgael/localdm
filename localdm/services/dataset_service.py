# Standard library
from datetime import UTC, datetime
from typing import TYPE_CHECKING

# Third-party
import polars as pl

# Local imports
from localdm.core.models import DatasetMetadata, DatasetStats
from localdm.core.storage import get_object_path
from localdm.core.utils import compute_hash, compute_stats, extract_schema
from localdm.core.validation import (
    validate_dataframe,
    validate_dataset_name,
    validate_description,
    validate_tag_name,
)
from localdm.repositories.data_repository import DataRepository
from localdm.repositories.metadata_repository import MetadataRepository

if TYPE_CHECKING:
    from pathlib import Path

# -----------------------------
# Dataset Service
# -----------------------------


class DatasetService:
    """Service for dataset creation and derivation business logic."""

    def __init__(
        self, data_repo: DataRepository, metadata_repo: MetadataRepository
    ) -> None:
        self.data_repo = data_repo
        self.metadata_repo = metadata_repo

    # -----------------------------
    # Dataset Creation
    # -----------------------------

    def create_dataset(
        self,
        name: str,
        data: pl.DataFrame,
        tag: str | None = None,
        parent_refs: list[str] | None = None,
        description: str | None = None,
        author: str | None = None,
    ) -> DatasetMetadata:
        """Create new dataset with business logic.

        Args:
            name: Dataset name
            data: DataFrame to save
            tag: Optional tag name
            parent_refs: Optional list of parent references
            description: Optional description (can be detailed, multiline text)
            author: Optional author (defaults to current username)

        Returns:
            DatasetMetadata for created dataset
        """
        # Validate inputs
        validate_dataset_name(name)
        validate_dataframe(data)
        validate_description(description)
        if tag:
            validate_tag_name(tag)

        # Auto-detect author if not provided
        if author is None:
            author = self._get_default_author()

        # Build metadata with business rules
        metadata: DatasetMetadata = self._create_metadata(
            data=data,
            name=name,
            tag=tag,
            parent_refs=parent_refs or [],
            author=author,
            description=description,
        )

        # Persist data via DataRepository
        self.data_repo.save_data(data, metadata)

        # Persist metadata via MetadataRepository
        self.metadata_repo.save(metadata)

        return metadata

    def derive_dataset(
        self,
        source_ref: str,
        data: pl.DataFrame,
        name: str | None = None,
        tag: str | None = None,
        description: str | None = None,
    ) -> DatasetMetadata:
        """Create dataset derived from source.

        Args:
            source_ref: Source dataset reference
            data: New data for derived dataset
            name: Optional new name (defaults to source name)
            tag: Optional tag for derived dataset
            description: Optional description (can be detailed, multiline text)

        Returns:
            DatasetMetadata for derived dataset
        """
        # Get source metadata for name and to establish lineage
        source_id: str = self.metadata_repo.resolve_ref_to_id(source_ref)
        source_meta: DatasetMetadata = self.metadata_repo.load(source_id)

        # Create derived dataset with source as parent (using ID)
        return self.create_dataset(
            name=name or source_meta.name,
            data=data,
            tag=tag,
            parent_refs=[source_id],
            description=description,
        )

    def update_dataset(
        self,
        dataset_id: str,
        data: pl.DataFrame,
        description: str | None = None,
    ) -> DatasetMetadata:
        """Update existing dataset with new data.

        Replaces data and hash but keeps same ID, tags, and lineage.
        Updates schema and stats automatically.

        Args:
            dataset_id: Dataset UUID
            data: New DataFrame
            description: Optional new description (or keep existing if None)

        Returns:
            Updated DatasetMetadata

        Example:
            # Load, modify, update
            meta = metadata_repo.load(dataset_id)
            df = data_repo.load_data(meta.hash).collect()
            df = df.with_columns(pl.col("age").cast(pl.Float64))
            update_dataset(dataset_id, df)
        """
        # Validate inputs
        validate_dataframe(data)
        validate_description(description)

        # Load existing metadata
        old_metadata: DatasetMetadata = self.metadata_repo.load(dataset_id)

        # Compute new hash and metadata
        new_hash: str = compute_hash(data)
        new_schema: dict[str, str] = extract_schema(data)
        new_stats: DatasetStats = compute_stats(data)
        new_data_path: Path = get_object_path(self.data_repo.repo_path, new_hash)

        # Delete old parquet file if hash changed
        if new_hash != old_metadata.hash:
            self.data_repo.delete_data(old_metadata.hash)

        # Save new parquet file
        # Create a temporary metadata object just for saving data
        temp_metadata = DatasetMetadata(
            id=dataset_id,
            hash=new_hash,
            name=old_metadata.name,
            tags=[],
            created_at=old_metadata.created_at,
            updated_at=datetime.now(UTC).isoformat(),
            author=old_metadata.author,
            parent_refs=[],
            description=None,
            schema=None,
            stats=None,
            data_path=str(new_data_path),
        )
        self.data_repo.save_data(data, temp_metadata)

        # Create updated metadata (keeps ID, tags, lineage)
        updated_metadata = DatasetMetadata(
            id=old_metadata.id,  # SAME ID
            hash=new_hash,  # NEW HASH
            name=old_metadata.name,  # SAME NAME
            tags=old_metadata.tags,  # SAME TAGS
            created_at=old_metadata.created_at,  # ORIGINAL CREATION
            updated_at=datetime.now(UTC).isoformat(),  # NEW UPDATE TIME
            author=old_metadata.author,
            parent_refs=old_metadata.parent_refs,  # SAME LINEAGE
            description=description
            if description is not None
            else old_metadata.description,
            schema=new_schema,  # NEW SCHEMA
            stats=new_stats,  # NEW STATS
            data_path=str(new_data_path),
        )

        # Update metadata in database
        self.metadata_repo.save(updated_metadata)

        return updated_metadata

    # -----------------------------
    # Business Logic Helpers
    # -----------------------------

    def _get_default_author(self) -> str:
        """Get default author (current system username).

        Returns:
            Current username or 'unknown'
        """
        import getpass

        try:
            return getpass.getuser()
        except Exception:  # noqa: BLE001
            return "unknown"

    def _create_metadata(
        self,
        data: pl.DataFrame,
        name: str,
        tag: str | None,
        parent_refs: list[str],
        author: str,
        description: str | None,
    ) -> DatasetMetadata:
        """Create DatasetMetadata with business rules.

        Business rules:
        - Generate hash from data
        - Extract schema and stats from data
        - Use current UTC timestamp
        - Compute data path from hash

        Args:
            data: DataFrame to analyze
            name: Dataset name
            tag: Optional tag
            parent_refs: List of parent references
            author: Dataset author
            description: Optional description

        Returns:
            DatasetMetadata with all fields populated
        """
        # Generate ID and compute metadata
        dataset_id: str = self.metadata_repo.generate_id()
        hash_val: str = compute_hash(data)
        schema: dict[str, str] = extract_schema(data)
        stats: DatasetStats = compute_stats(data)
        data_path: Path = get_object_path(self.data_repo.repo_path, hash_val)
        timestamp: str = datetime.now(UTC).isoformat()

        return DatasetMetadata(
            id=dataset_id,
            hash=hash_val,
            name=name,
            tags=[tag] if tag else [],
            created_at=timestamp,
            updated_at=timestamp,
            author=author,
            parent_refs=parent_refs,
            description=description,
            schema=schema,
            stats=stats,
            data_path=str(data_path),
        )

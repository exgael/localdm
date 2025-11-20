# Standard library
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

# Local imports
from localdm.core.models import DatasetMetadata
from localdm.repositories.schemas import (
    SQL_CREATE_DATASETS,
    SQL_CREATE_INDEXES,
    SQL_CREATE_LINEAGE,
    SQL_CREATE_TAGS,
)

# -----------------------------
# Constants
# -----------------------------

UUID_STRING_LENGTH = 36

# -----------------------------
# Metadata Repository
# -----------------------------


class MetadataRepository:
    """Repository for dataset metadata CRUD operations."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    # -----------------------------
    # Database Initialization
    # -----------------------------

    def init_database(self) -> None:
        """Initialize SQLite database with schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            conn.execute(SQL_CREATE_DATASETS)
            conn.execute(SQL_CREATE_LINEAGE)
            conn.execute(SQL_CREATE_TAGS)
            conn.executescript(SQL_CREATE_INDEXES)
            conn.commit()
        finally:
            conn.close()

    # -----------------------------
    # CRUD Operations
    # -----------------------------

    def save(self, metadata: DatasetMetadata) -> None:
        """Save dataset metadata to database."""
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            # Save dataset
            conn.execute(
                """
                INSERT OR REPLACE INTO datasets
                (id, hash, name, created_at, updated_at, author, data_path,
                 description, schema_json, stats_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metadata.id,
                    metadata.hash,
                    metadata.name,
                    metadata.created_at,
                    metadata.updated_at,
                    metadata.author,
                    metadata.data_path,
                    metadata.description,
                    json.dumps(metadata.schema) if metadata.schema else None,
                    json.dumps(metadata.stats) if metadata.stats else None,
                ),
            )

            # Save tags
            for tag in metadata.tags:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO tags (name, tag, dataset_id, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (metadata.name, tag, metadata.id, metadata.created_at),
                )

            # Save lineage (parent_refs are now IDs)
            for parent_id in metadata.parent_refs:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO lineage (child_id, parent_id)
                    VALUES (?, ?)
                    """,
                    (metadata.id, parent_id),
                )

            conn.commit()
        finally:
            conn.close()

    def generate_id(self) -> str:
        """Generate a new unique dataset ID.

        Returns:
            UUID string
        """
        return str(uuid.uuid4())

    def load(self, dataset_id: str) -> DatasetMetadata:
        """Load dataset metadata by ID.

        Args:
            dataset_id: Dataset UUID

        Returns:
            DatasetMetadata

        Raises:
            KeyError: If dataset not found
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            # Load dataset
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT id, hash, name, created_at, updated_at, author, data_path,
                       description, schema_json, stats_json
                FROM datasets
                WHERE id = ?
                """,
                (dataset_id,),
            )

            row: Any = cursor.fetchone()
            if not row:
                msg = f"Dataset with ID '{dataset_id}' not found"
                raise KeyError(msg)

            (
                id_val,
                hash_val,
                name,
                created_at,
                updated_at,
                author,
                data_path,
                description,
                schema_json,
                stats_json,
            ) = row

            # Load tags
            tags_cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT tag FROM tags
                WHERE dataset_id = ?
                ORDER BY created_at
                """,
                (dataset_id,),
            )
            tags: list[Any] = [row[0] for row in tags_cursor.fetchall()]

            # Load parent IDs
            parents_cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT parent_id FROM lineage
                WHERE child_id = ?
                """,
                (dataset_id,),
            )
            parent_ids: list[Any] = [row[0] for row in parents_cursor.fetchall()]

            return DatasetMetadata(
                id=id_val,
                hash=hash_val,
                name=name,
                tags=tags,
                created_at=created_at,
                updated_at=updated_at,
                author=author,
                parent_refs=parent_ids,
                description=description,
                schema=json.loads(schema_json) if schema_json else None,
                stats=json.loads(stats_json) if stats_json else None,
                data_path=data_path,
            )
        finally:
            conn.close()

    def resolve_ref_to_id(self, ref: str) -> str:
        """Resolve dataset reference to ID.

        Args:
            ref: Reference in format name:tag, name@hash, or UUID

        Returns:
            Dataset ID (UUID)

        Raises:
            KeyError: If reference not found
            ValueError: If reference format invalid
        """
        # If it looks like a UUID, use it directly
        if "-" in ref and len(ref) == UUID_STRING_LENGTH:
            # Validate it exists
            conn: sqlite3.Connection = sqlite3.connect(self.db_path)
            try:
                cursor: sqlite3.Cursor = conn.execute(
                    "SELECT id FROM datasets WHERE id = ?",
                    (ref,),
                )
                if cursor.fetchone():
                    return ref
            finally:
                conn.close()

        if "@" in ref:
            # Hash reference
            _, hash_val = ref.split("@", 1)
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "SELECT id FROM datasets WHERE hash = ?",
                    (hash_val,),
                )
                row: Any = cursor.fetchone()
                if not row:
                    msg = f"Dataset with hash '{hash_val}' not found"
                    raise KeyError(msg)
                dataset_id_result: str = row[0]
                return dataset_id_result
            finally:
                conn.close()

        if ":" in ref:
            # Tag reference
            name, tag = ref.split(":", 1)
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    """
                    SELECT dataset_id FROM tags
                    WHERE name = ? AND tag = ?
                    """,
                    (name, tag),
                )
                row = cursor.fetchone()
                if not row:
                    msg = f"Tag '{tag}' not found for dataset '{name}'"
                    raise KeyError(msg)
                tag_dataset_id: str = row[0]
                return tag_dataset_id
            finally:
                conn.close()

        msg = f"Invalid reference '{ref}'. Use 'name:tag', 'name@hash', or ID format."
        raise ValueError(msg)

    def list_all_refs(self) -> list[str]:
        """List all dataset references in database."""
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT DISTINCT name, tag, hash FROM tags
                ORDER BY name, tag
                """
            )
            return [f"{name}:{tag}" for name, tag, _ in cursor.fetchall()]
        finally:
            conn.close()

    # -----------------------------
    # Dataset Listing & Filtering
    # -----------------------------

    def list_datasets(
        self,
        name_filter: str | None = None,
        tag_filter: str | None = None,
        limit: int | None = None,
    ) -> list[DatasetMetadata]:
        """List datasets with optional filtering.

        Args:
            name_filter: Optional name pattern to filter by (SQL LIKE pattern)
            tag_filter: Optional tag to filter by (exact match)
            limit: Optional maximum number of results

        Returns:
            List of DatasetMetadata, ordered by creation time (newest first)
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            if tag_filter:
                # Join with tags table if filtering by tag
                query: str = """
                    SELECT DISTINCT d.id
                    FROM datasets d
                    INNER JOIN tags t ON d.id = t.dataset_id
                    WHERE t.tag = ?
                """
                params: list[str | int] = [tag_filter]

                if name_filter:
                    query += " AND d.name LIKE ?"
                    params.append(f"%{name_filter}%")

                query += " ORDER BY d.created_at DESC"
            else:
                query = "SELECT DISTINCT id FROM datasets WHERE 1=1"
                params = []

                if name_filter:
                    query += " AND name LIKE ?"
                    params.append(f"%{name_filter}%")

                query += " ORDER BY created_at DESC"

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            cursor: sqlite3.Cursor = conn.execute(query, params)
            dataset_ids: list[str] = [row[0] for row in cursor.fetchall()]

            return [self.load(dataset_id) for dataset_id in dataset_ids]
        finally:
            conn.close()

    # -----------------------------
    # Name update
    # -----------------------------

    def update_name(self, dataset_id: str, new_name: str) -> None:
        """Update dataset name.

        Args:
            dataset_id: Dataset UUID
            new_name: New dataset name
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "UPDATE datasets SET name = ? WHERE id = ?",
                (new_name, dataset_id),
            )
            conn.commit()
        finally:
            conn.close()

    # -----------------------------
    # Description update
    # -----------------------------

    def update_description(self, dataset_id: str, new_description: str) -> None:
        """Update dataset description.

        Args:
            dataset_id: Dataset UUID
            new_description: New description text
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "UPDATE datasets SET description = ? WHERE id = ?",
                (new_description, dataset_id),
            )
            conn.commit()
        finally:
            conn.close()

    # -----------------------------
    # Tag Operations
    # -----------------------------

    def add_tag(self, dataset_id: str, tag: str) -> None:
        """Add a tag to an existing dataset.

        Args:
            dataset_id: Dataset UUID
            tag: Tag name to add

        Raises:
            KeyError: If dataset not found
        """
        from datetime import UTC, datetime

        # Get dataset name
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                "SELECT name FROM datasets WHERE id = ?", (dataset_id,)
            )
            row: Any = cursor.fetchone()
            if not row:
                msg = f"Dataset with ID '{dataset_id}' not found"
                raise KeyError(msg)

            name: str = row[0]

            # Check if tag already exists
            existing: sqlite3.Cursor = conn.execute(
                "SELECT 1 FROM tags WHERE name = ? AND tag = ?", (name, tag)
            )
            if existing.fetchone():
                return  # Tag already exists, nothing to do

            # Insert new tag
            conn.execute(
                """
                INSERT INTO tags (name, tag, dataset_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (name, tag, dataset_id, datetime.now(UTC).isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    def remove_tag(self, dataset_id: str, tag: str) -> None:
        """Remove a tag from a dataset.

        Args:
            dataset_id: Dataset UUID
            tag: Tag name to remove

        Raises:
            KeyError: If dataset or tag not found
        """

        # Get dataset name
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                "SELECT name FROM datasets WHERE id = ?", (dataset_id,)
            )
            row: Any = cursor.fetchone()
            if not row:
                msg = f"Dataset with ID '{dataset_id}' not found"
                raise KeyError(msg)

            name: str = row[0]

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

    def list_tags(self, dataset_id: str) -> list[tuple[str, str]]:
        """List all tags for a dataset with timestamps.

        Args:
            dataset_id: Dataset UUID

        Returns:
            List of (tag, created_at) tuples, ordered by creation time (newest first)
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT tag, created_at
                FROM tags
                WHERE dataset_id = ?
                ORDER BY created_at DESC
                """,
                (dataset_id,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]
        finally:
            conn.close()

    # -----------------------------
    # Deletion
    # -----------------------------

    def delete_metadata(self, dataset_id: str) -> None:
        """Delete all metadata for a dataset.

        Args:
            dataset_id: Dataset ID

        Note:
            Cascades to tags and lineage via foreign keys (ON DELETE CASCADE)
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            conn.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))
            conn.commit()
        finally:
            conn.close()

    def get_children(self, dataset_id: str) -> list[DatasetMetadata]:
        """Get all child datasets.

        Args:
            dataset_id: Parent dataset ID

        Returns:
            List of child dataset metadata
        """
        conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT child_id FROM lineage
                WHERE parent_id = ?
                """,
                (dataset_id,),
            )
            child_ids: list[str] = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

        return [self.load(child_id) for child_id in child_ids]

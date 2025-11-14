# Standard library
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# -----------------------------
# TypedDict definitions
# -----------------------------


@dataclass(frozen=True)
class DatasetMetadata:
    """Immutable dataset metadata."""

    hash: str
    name: str
    tags: list[str]
    created_at: str
    parent_refs: list[str]
    transform_type: str | None
    transform_metadata: dict[str, object] | None
    schema: dict[str, str] | None
    stats: dict[str, object] | None
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
        return f"Metadata({self.name}{tags}, hash={self.hash[:7]}...)"


# -----------------------------
# SQLite database operations
# -----------------------------

SQL_CREATE_DATASETS = """
CREATE TABLE IF NOT EXISTS datasets (
    hash TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data_path TEXT NOT NULL,
    transform_type TEXT,
    transform_metadata_json TEXT,
    schema_json TEXT,
    stats_json TEXT
);
"""

SQL_CREATE_LINEAGE = """
CREATE TABLE IF NOT EXISTS lineage (
    child_hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    PRIMARY KEY (child_hash, parent_hash),
    FOREIGN KEY (child_hash) REFERENCES datasets(hash),
    FOREIGN KEY (parent_hash) REFERENCES datasets(hash)
);
"""

SQL_CREATE_TAGS = """
CREATE TABLE IF NOT EXISTS tags (
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (name, tag),
    FOREIGN KEY (hash) REFERENCES datasets(hash)
);
"""

SQL_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets(name);
CREATE INDEX IF NOT EXISTS idx_tags_hash ON tags(hash);
CREATE INDEX IF NOT EXISTS idx_lineage_child ON lineage(child_hash);
CREATE INDEX IF NOT EXISTS idx_lineage_parent ON lineage(parent_hash);
"""


def init_database(db_path: Path) -> None:
    """Initialize SQLite database with schema."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn: sqlite3.Connection = sqlite3.connect(db_path)
    try:
        conn.execute(SQL_CREATE_DATASETS)
        conn.execute(SQL_CREATE_LINEAGE)
        conn.execute(SQL_CREATE_TAGS)
        conn.executescript(SQL_CREATE_INDEXES)
        conn.commit()
    finally:
        conn.close()


def save_metadata(db_path: Path, metadata: DatasetMetadata) -> None:
    """Save dataset metadata to database."""
    conn: sqlite3.Connection = sqlite3.connect(db_path)
    try:
        # Save dataset
        conn.execute(
            """
            INSERT OR REPLACE INTO datasets
            (hash, name, created_at, data_path, transform_type,
             transform_metadata_json, schema_json, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metadata.hash,
                metadata.name,
                metadata.created_at,
                metadata.data_path,
                metadata.transform_type,
                json.dumps(metadata.transform_metadata)
                if metadata.transform_metadata
                else None,
                json.dumps(metadata.schema) if metadata.schema else None,
                json.dumps(metadata.stats) if metadata.stats else None,
            ),
        )

        # Save tags
        for tag in metadata.tags:
            conn.execute(
                """
                INSERT OR REPLACE INTO tags (name, tag, hash, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (metadata.name, tag, metadata.hash, metadata.created_at),
            )

        # Save lineage
        for parent_ref in metadata.parent_refs:
            parent_hash: str = resolve_ref_to_hash(db_path, parent_ref)
            conn.execute(
                """
                INSERT OR IGNORE INTO lineage (child_hash, parent_hash)
                VALUES (?, ?)
                """,
                (metadata.hash, parent_hash),
            )

        conn.commit()
    finally:
        conn.close()


def load_metadata(db_path: Path, hash_val: str) -> DatasetMetadata:
    """Load dataset metadata by hash."""
    conn: sqlite3.Connection = sqlite3.connect(db_path)
    try:
        # Load dataset
        cursor: sqlite3.Cursor = conn.execute(
            """
            SELECT name, created_at, data_path, transform_type,
                   transform_metadata_json, schema_json, stats_json
            FROM datasets
            WHERE hash = ?
            """,
            (hash_val,),
        )

        row: Any = cursor.fetchone()
        if not row:
            msg = f"Dataset with hash '{hash_val}' not found"
            raise KeyError(msg)

        (
            name,
            created_at,
            data_path,
            transform_type,
            transform_metadata_json,
            schema_json,
            stats_json,
        ) = row

        # Load tags
        tags_cursor: sqlite3.Cursor = conn.execute(
            """
            SELECT tag FROM tags
            WHERE name = ? AND hash = ?
            ORDER BY created_at
            """,
            (name, hash_val),
        )
        tags: list[Any] = [row[0] for row in tags_cursor.fetchall()]

        # Load parent refs
        parents_cursor: sqlite3.Cursor = conn.execute(
            """
            SELECT parent_hash FROM lineage
            WHERE child_hash = ?
            """,
            (hash_val,),
        )
        parent_hashes: list[Any] = [row[0] for row in parents_cursor.fetchall()]

        # Convert parent hashes to refs (prefer tags if available)
        parent_refs: list[str] = [_hash_to_ref(conn, h) for h in parent_hashes]

        return DatasetMetadata(
            hash=hash_val,
            name=name,
            tags=tags,
            created_at=created_at,
            parent_refs=parent_refs,
            transform_type=transform_type,
            transform_metadata=json.loads(transform_metadata_json)
            if transform_metadata_json
            else None,
            schema=json.loads(schema_json) if schema_json else None,
            stats=json.loads(stats_json) if stats_json else None,
            data_path=data_path,
        )
    finally:
        conn.close()


def resolve_ref_to_hash(db_path: Path, ref: str) -> str:
    """Resolve dataset reference to hash."""
    if "@" in ref:
        # Direct hash reference
        _, hash_val = ref.split("@", 1)
        return hash_val

    if ":" in ref:
        # Tag reference
        name, tag = ref.split(":", 1)
        conn: sqlite3.Connection = sqlite3.connect(db_path)
        try:
            cursor: sqlite3.Cursor = conn.execute(
                """
                SELECT hash FROM tags
                WHERE name = ? AND tag = ?
                """,
                (name, tag),
            )
            row: Any = cursor.fetchone()
            if not row:
                msg = f"Tag '{tag}' not found for dataset '{name}'"
                raise KeyError(msg)
            hash_value: str = row[0]
            return hash_value
        finally:
            conn.close()

    msg = f"Invalid reference '{ref}'. Use 'name:tag' or 'name@hash' format."
    raise ValueError(msg)


def _hash_to_ref(conn: sqlite3.Connection, hash_val: str) -> str:
    """Convert hash to preferred reference (tag if available, else hash)."""
    cursor: sqlite3.Cursor = conn.execute(
        """
        SELECT name, tag FROM tags
        WHERE hash = ?
        ORDER BY created_at
        LIMIT 1
        """,
        (hash_val,),
    )
    row: Any = cursor.fetchone()
    if row:
        name, tag = row
        return f"{name}:{tag}"

    # No tag, use hash
    cursor = conn.execute(
        """
        SELECT name FROM datasets
        WHERE hash = ?
        """,
        (hash_val,),
    )
    row2: Any = cursor.fetchone()
    if row2:
        dataset_name: str = row2[0]
        return f"{dataset_name}@{hash_val[:7]}"

    return f"@{hash_val[:7]}"


def list_all_dataset_refs(db_path: Path) -> list[str]:
    """List all dataset references in database."""
    conn: sqlite3.Connection = sqlite3.connect(db_path)
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

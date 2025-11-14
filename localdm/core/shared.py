# Standard library
import hashlib
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Literal

# Third-party
import polars as pl

# Local imports
from localdm.core.metadata import DatasetMetadata
from localdm.types import EngineType

# -----------------------------
# Metadata utilities
# -----------------------------


def create_metadata(
    hash_val: str,
    name: str,
    tags: list[str],
    parent_refs: list[str],
    engine: EngineType,
    transform_type: str | None,
    transform_metadata: dict[str, object] | None,
    schema: dict[str, str],
    stats: dict[str, object],
    data_path: str,
) -> DatasetMetadata:
    """Create DatasetMetadata with current timestamp."""
    return DatasetMetadata(
        hash=hash_val,
        name=name,
        tags=tags,
        created_at=datetime.now().isoformat(),
        parent_refs=parent_refs,
        engine=engine,
        transform_type=transform_type,
        transform_metadata=transform_metadata,
        schema=schema,
        stats=stats,
        data_path=data_path,
    )


def extract_transform_type(metadata: dict[str, object] | None) -> str | None:
    """Extract transform type from metadata dict."""
    if metadata and "transform" in metadata:
        transform_val = metadata["transform"]
        return str(transform_val) if transform_val is not None else None
    return None


def get_parent_ref_by_name(parent_refs: list[str], name: str) -> str:
    """Find parent reference by dataset name."""
    for ref in parent_refs:
        if ref.startswith((f"{name}:", f"{name}@")):
            return ref
    msg = f"Parent '{name}' not found"
    raise KeyError(msg)


# -----------------------------
# File I/O utilities
# -----------------------------


def load_file(path: Path) -> pl.DataFrame:
    """Auto-detect and load file as Polars DataFrame."""
    suffix: str = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix == ".parquet":
        return pl.read_parquet(path)
    if suffix in [".json", ".jsonl"]:
        return pl.read_json(path)
    msg: str = f"Unsupported file type: {suffix}"
    raise ValueError(msg)


# -----------------------------
# Hash computation utilities
# -----------------------------


def compute_hash(df: pl.DataFrame, *, full: bool = False) -> str:
    """Compute content hash of Polars DataFrame."""
    if full:
        return _compute_full_hash(df)
    return _compute_heuristic_hash(df)


def _compute_heuristic_hash(df: pl.DataFrame) -> str:
    """Fast hash based on metadata and samples."""
    components: list[str] = []

    # Shape
    components.append(f"rows:{len(df)}")
    components.append(f"cols:{len(df.columns)}")

    # Schema
    schema: list[tuple[str, str]] = sorted(
        (col, str(dtype)) for col, dtype in df.schema.items()
    )
    components.append(f"schema:{schema}")

    # Nulls
    null_total: int = df.null_count().sum_horizontal().item()
    components.append(f"nulls:{null_total}")

    # Sample head/tail
    head: str = df.head(5).write_csv()
    tail: str = df.tail(5).write_csv()
    components.append(f"head:{hashlib.sha256(head.encode()).hexdigest()[:8]}")
    components.append(f"tail:{hashlib.sha256(tail.encode()).hexdigest()[:8]}")

    # Numeric checksum
    numeric_cols: list[str] = [
        col
        for col, dtype in df.schema.items()
        if dtype in {pl.Int32, pl.Int64, pl.Float32, pl.Float64}
    ]
    if numeric_cols:
        total_row = df.select([pl.sum(col) for col in numeric_cols]).row(0)
        checksum: float | Literal[0] = sum(
            float(v) for v in total_row if v is not None
        )
        components.append(f"numeric_sum:{checksum}")

    combined = "|".join(components)
    return hashlib.sha256(combined.encode()).hexdigest()


def _compute_full_hash(df: pl.DataFrame) -> str:
    """Full hash of entire dataframe (slow but accurate)."""
    buffer_io = BytesIO()
    df.write_parquet(buffer_io)
    buffer: bytes = buffer_io.getvalue()
    return hashlib.sha256(buffer).hexdigest()


# -----------------------------
# Schema and stats utilities
# -----------------------------


def extract_schema(df: pl.DataFrame) -> dict[str, str]:
    """Extract schema from Polars DataFrame."""
    return {col: str(dtype) for col, dtype in df.schema.items()}


def compute_stats(df: pl.DataFrame) -> dict[str, object]:
    """Compute statistics for Polars DataFrame."""
    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "null_counts": {col: df[col].null_count() for col in df.columns},
    }

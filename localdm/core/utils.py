# Standard library
import hashlib
from io import BytesIO
from pathlib import Path
from typing import cast

# Third-party
import polars as pl

# Local imports
from localdm.core.models import ColumnStats, DatasetStats

# -----------------------------
# Constants
# -----------------------------

APPROX_UNIQUE_THRESHOLD = 10_000

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

    # Sample head/tail (only 5 rows each - very cheap)
    head: str = df.head(5).write_parquet()
    tail: str = df.tail(5).write_parquet()
    components.append(f"head:{hashlib.sha256(head.encode()).hexdigest()[:8]}")
    components.append(f"tail:{hashlib.sha256(tail.encode()).hexdigest()[:8]}")

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


def compute_stats(df: pl.DataFrame) -> DatasetStats:
    """Compute enhanced statistics for Polars DataFrame.

    Returns:
        DatasetStats with:
        - row_count: Total number of rows
        - column_count: Total number of columns
        - column_stats: Per-column statistics (null %, unique count)
    """
    column_stats: dict[str, ColumnStats] = {}

    for col in df.columns:
        null_count: int = df[col].null_count()
        null_percentage: float = (
            (null_count / df.height * 100) if df.height > 0 else 0.0
        )

        # Unique count (use approx for large datasets)
        if df.height > APPROX_UNIQUE_THRESHOLD:
            approx_val = df[col].approx_n_unique()
            unique_count: int = cast("int", approx_val) if approx_val is not None else 0
        else:
            unique_count = df[col].n_unique()

        column_stats[col] = {
            "null_count": null_count,
            "null_percentage": null_percentage,
            "unique_count": unique_count,
        }

    return {
        "row_count": df.height,
        "column_count": len(df.columns),
        "column_stats": column_stats,
    }

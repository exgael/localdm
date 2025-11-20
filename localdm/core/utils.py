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

    # Helper to get parquet bytes
    def parquet_bytes(subdf: pl.DataFrame) -> bytes:
        buffer = BytesIO()
        subdf.write_parquet(buffer)
        return buffer.getvalue()

    # Sample head/tail (only 5 rows each - very cheap)
    head_bytes = parquet_bytes(df.head(5))
    tail_bytes = parquet_bytes(df.tail(5))

    components.append(f"head:{hashlib.sha256(head_bytes).hexdigest()[:8]}")
    components.append(f"tail:{hashlib.sha256(tail_bytes).hexdigest()[:8]}")

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

    # Dtypes that support approx_n_unique
    APPROX_SUPPORTED = {
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        pl.Float32, pl.Float64,
        pl.Boolean,
        pl.String,
    }

    for col in df.columns:
        series = df[col]
        col_dtype = series.dtype

        null_count: int = series.null_count()
        null_percentage: float = (
            (null_count / df.height * 100) if df.height > 0 else 0.0
        )

        # Unique count with safe approx fallback
        if (
            df.height > APPROX_UNIQUE_THRESHOLD
            and col_dtype in APPROX_SUPPORTED
        ):
            approx_val = series.approx_n_unique()
            unique_count: int = int(approx_val) if approx_val is not None else 0
        else:
            unique_count = series.n_unique()

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

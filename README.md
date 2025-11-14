# LocalDM

Memory-efficient, type-safe data versioning library with lineage tracking using Polars.

## Installation

### From GitHub

```bash
uv add git+https://github.com/exgael/localdm.git
```

## Quick Start

```python
from localdm import DataManager
import polars as pl

# Initialize data manager
dm = DataManager(repo_path="./.localdm")

# Load data yourself with Polars
users_df = pl.read_csv("users.csv")

# Create a versioned dataset
users = dm.create_dataset(
    name="users",
    data=users_df,
    tag="v1"
)

# Work with data - users.df returns a LazyFrame (no data loaded yet!)
lazy = users.df
result = lazy.filter(pl.col("age") >= 30).select(["name", "age"]).collect()

# Create a derived dataset with lineage tracking
adults_df = users_df.filter(pl.col("age") >= 30).collect()
adults = dm.create_dataset(
    name="adults",
    data=adults_df,
    tag="v1",
    parents=[users],
    metadata={"transform": "age_filter", "threshold": 30}
)

# Retrieve datasets by reference
users_v1 = dm.get("users:v1")

# Access data lazily - only loads when you call .collect()
lazy_df = users_v1.df
materialized = lazy_df.collect()
```

## Core Concepts

### DataManager

Central manager for creating and retrieving versioned datasets.

```python
dm = DataManager(repo_path="./.localdm")
```

**Key Method:**
- `create_dataset(name, data, tag=None, parents=None, metadata=None)` - Version a Polars DataFrame

### Dataset

Immutable dataset reference with metadata and lazy data access.

```python
# Access data lazily (returns LazyFrame)
lazy_df = dataset.df

# Chain operations without loading data
result = dataset.df.filter(...).select(...).collect()

# Or load everything at once
df = dataset.df.collect()
```

**Memory Efficiency:**
- `Dataset.df` returns a `LazyFrame` - no data loaded until `.collect()`
- Datasets don't cache data internally
- You control when data materializes

### Lineage Tracking

Track relationships between datasets:

```python
# Create dataset with parents
derived = dm.create_dataset(
    name="derived",
    data=result_df,
    parents=[dataset1, dataset2],
    metadata={"transform": "join"}
)

# Get specific parent
parent = dataset.get_parent("parent_name", dm.repository)

# Get all parents
parents = dataset.get_parents(dm.repository)
```

### References

Datasets can be referenced by `name:tag` or `name@hash`:

```python
dm.get("users:v1")           # By tag
dm.get("users@abc123...")    # By hash (first 7 chars)
```

## Philosophy

**localdm is a middleman between you and Polars:**
- You manage your DataFrames
- localdm manages versions, hashes, and lineage
- Lazy loading by default for memory efficiency
- Fast approximate hashing (schema + dimensions + samples)

## License

MIT

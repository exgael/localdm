# LocalDM

Type-safe data versioning library with lineage tracking using Polars.

## Installation

```bash
uv add git+https://github.com/exgael/localdm.git
```

## Quick Start

```python
from localdm import DataManager
import polars as pl

# Initialize data manager
dm = DataManager(repo_path="./.localdm")

# Load data with Polars
users_df = pl.read_csv("users.csv")

# Create a versioned dataset (returns reference string)
users_ref = dm.create_dataset(
    name="users",
    data=users_df,
    tag="v1"
)

# Get dataset as LazyFrame
lazy = dm.get(users_ref)
result = lazy.filter(pl.col("age") >= 30).select(["name", "age"]).collect()

# Create derived dataset with lineage
adults_ref = dm.create_dataset(
    name="adults",
    data=result,
    tag="v1",
    parents=[users_ref],
    description="Users age >= 30"
)

# Display datasets
dm.show_all()                    # Table of all datasets
dm.show(users_ref)               # Detailed info panel
dm.visualize_lineage(adults_ref) # Lineage tree
```

## API Reference

### Initialize

```python
dm = DataManager(repo_path="./.localdm")
```

Creates or connects to a LocalDM repository.

### Create Datasets

```python
ref = dm.create_dataset(
    name="dataset_name",
    data=df,                    # Polars DataFrame
    tag="v1",                   # Optional: tag for this version
    parents=["parent:v1"],      # Optional: parent references for lineage
    description="...",          # Optional: dataset description
    author="username"           # Optional: defaults to system username
)
```

Returns a reference string (e.g., `"users:v1"`).

### Derive from Existing Dataset

```python
ref = dm.derive_dataset(
    source_ref="users:v1",
    data=transformed_df,
    name="new_name",            # Optional: defaults to source name
    tag="v2",                   # Optional
    description="..."           # Optional
)
```

Automatically sets the source as parent.

### Update Dataset

```python
dm.update_dataset(
    dataset_id=meta.id,
    data=new_df,
    description="..."           # Optional
)
```

Replaces data while preserving ID, tags, and lineage.

### Get Data

```python
lazy_df = dm.get(ref)          # Returns Polars LazyFrame
df = dm.get(ref).collect()     # Materialize data
```

References can be:
- `"name:tag"` - by tag
- `"name@hash"` - by hash prefix
- Dataset ID (UUID)

### List Datasets

```python
all_datasets = dm.list_datasets()
filtered = dm.list_datasets(name_filter="user", tag_filter="v1")
```

Returns list of `DatasetMetadata` objects.

### Display

```python
dm.show_all()                  # Rich table of all datasets
dm.show_all(name_filter="user")

dm.show(ref)                   # Detailed panel for one dataset
dm.visualize_lineage(ref)      # Parent dependency tree
dm.tree()                      # Full repository lineage tree
```

### Metadata Operations

```python
# Tags
dm.add_tag(dataset_id, "new_tag")
dm.remove_tag(dataset_id, "old_tag")
dm.list_tags(dataset_id)       # Returns [(tag, created_at), ...]

# Update metadata
dm.update_name(dataset_id, "new_name")
dm.update_description(dataset_id, "new description")
```

### Delete

```python
dm.delete(dataset_id)          # Warns if dataset has children
dm.delete(dataset_id, force=True)  # Force delete
```

## License

MIT

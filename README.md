# LocalDM

Type-safe data versioning library with lineage tracking using Polars.

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

# Load a dataset from file
users = dm.load("users.csv", name="users", tag="v1")

# Access the underlying Polars DataFrame
print(users.df)

# Create a derived dataset
adults_df = users.df.filter(pl.col("age") >= 30)
adults = users.derive(
    data=adults_df,
    manager=dm,
    name="adults",
    tag="v1",
    metadata={"transform": "age_filter", "threshold": 30}
)

# Create a new dataset from computed data
result_df = pl.DataFrame({"id": [1, 2], "value": [100, 200]})
result = dm.create_dataset(
    name="results",
    data=result_df,
    tag="v1",
    parents=[users, adults],
    metadata={"transform": "computation"}
)

# Retrieve datasets by reference
users_v1 = dm.get("users:v1")

# Explore lineage
parent = adults.get_parent("users", dm.repository)
all_parents = result.get_parents(dm.repository)
```

## Core Concepts

### DataManager

Central manager for loading, creating, and retrieving datasets.

```python
dm = DataManager(repo_path="./.localdm")
```

### Dataset

Immutable dataset object with metadata and lazy-loaded data.

```python
# Access data
df = dataset.df  # Returns Polars DataFrame

# Convert to Pandas if needed
pandas_df = dataset.df.to_pandas()

# Create derived dataset
derived = dataset.derive(data=new_df, manager=dm, name="derived", tag="v1")
```

### Lineage Tracking

Track relationships between datasets:

```python
# Get specific parent
parent = dataset.get_parent("parent_name", repository)

# Get all parents
parents = dataset.get_parents(repository)
```

### References

Datasets can be referenced by `name:tag` or `name@hash`:

```python
dm.get("users:v1")           # By tag
dm.get("users@abc123...")    # By hash
```

## License

MIT

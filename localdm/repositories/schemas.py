# SQL Schema Definitions

SQL_CREATE_DATASETS = """
CREATE TABLE IF NOT EXISTS datasets (
    id TEXT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    author TEXT NOT NULL DEFAULT 'unknown',
    data_path TEXT NOT NULL,
    description TEXT,
    schema_json TEXT,
    stats_json TEXT
);
"""

SQL_CREATE_LINEAGE = """
CREATE TABLE IF NOT EXISTS lineage (
    child_id TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    PRIMARY KEY (child_id, parent_id),
    FOREIGN KEY (child_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES datasets(id) ON DELETE CASCADE
);
"""

SQL_CREATE_TAGS = """
CREATE TABLE IF NOT EXISTS tags (
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (name, tag),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);
"""

SQL_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets(name);
CREATE INDEX IF NOT EXISTS idx_datasets_hash ON datasets(hash);
CREATE INDEX IF NOT EXISTS idx_tags_dataset_id ON tags(dataset_id);
CREATE INDEX IF NOT EXISTS idx_lineage_child ON lineage(child_id);
CREATE INDEX IF NOT EXISTS idx_lineage_parent ON lineage(parent_id);
"""

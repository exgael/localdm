# Standard library
import re

# Third-party
import polars as pl

# -----------------------------
# Validation Constants
# -----------------------------

VALID_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
MAX_DESCRIPTION_LENGTH = 10_000
MAX_NAME_LENGTH = 100
MAX_TAG_LENGTH = 50

# -----------------------------
# Validation Errors
# -----------------------------


class ValidationError(ValueError):
    """Raised when validation fails."""


# -----------------------------
# Validation Functions
# -----------------------------


def validate_dataset_name(name: str) -> None:
    """Validate dataset name format.

    Args:
        name: Dataset name to validate

    Raises:
        ValidationError: If name is invalid
    """
    if not name:
        msg = "Dataset name cannot be empty"
        raise ValidationError(msg)

    if not VALID_NAME_PATTERN.match(name):
        msg = (
            f"Invalid dataset name '{name}'. "
            "Only alphanumeric, underscore, and dash allowed."
        )
        raise ValidationError(msg)

    if len(name) > MAX_NAME_LENGTH:
        msg = f"Dataset name too long ({len(name)} chars, max {MAX_NAME_LENGTH})"
        raise ValidationError(msg)


def validate_tag_name(tag: str) -> None:
    """Validate tag name format.

    Args:
        tag: Tag name to validate

    Raises:
        ValidationError: If tag is invalid
    """
    if not tag:
        msg = "Tag name cannot be empty"
        raise ValidationError(msg)

    if not VALID_NAME_PATTERN.match(tag):
        msg = (
            f"Invalid tag name '{tag}'. "
            "Only alphanumeric, underscore, and dash allowed."
        )
        raise ValidationError(msg)

    if len(tag) > MAX_TAG_LENGTH:
        msg = f"Tag name too long ({len(tag)} chars, max {MAX_TAG_LENGTH})"
        raise ValidationError(msg)


def validate_dataframe(df: pl.DataFrame) -> None:
    """Validate DataFrame is usable.

    Args:
        df: DataFrame to validate

    Raises:
        ValidationError: If DataFrame is invalid
    """
    if df.is_empty():
        msg = "DataFrame is empty (0 rows)"
        raise ValidationError(msg)

    if len(df.columns) == 0:
        msg = "DataFrame has no columns"
        raise ValidationError(msg)


def validate_description(description: str | None) -> None:
    """Validate description length.

    Args:
        description: Description text to validate

    Raises:
        ValidationError: If description is too long
    """
    if description and len(description) > MAX_DESCRIPTION_LENGTH:
        msg = (
            f"Description too long "
            f"({len(description)} chars, max {MAX_DESCRIPTION_LENGTH})"
        )
        raise ValidationError(msg)


def validate_reference(ref: str) -> None:
    """Validate reference format.

    Args:
        ref: Reference string to validate

    Raises:
        ValidationError: If reference format is invalid
    """
    if not ref:
        msg = "Reference cannot be empty"
        raise ValidationError(msg)

    # Check format: name:tag or name@hash or just id
    if ":" in ref:
        name, tag = ref.split(":", 1)
        validate_dataset_name(name)
        validate_tag_name(tag)
    elif "@" in ref:
        name, _ = ref.split("@", 1)
        validate_dataset_name(name)
    # Otherwise assume it's an ID (UUID) - validated by DB lookup

"""
SAS Backend Helper Functions.

This module provides utility functions for the SAS backend.
"""
from typing import Dict, Any, Optional

import sqlalchemy as sa
from sqlalchemy.types import (
    INTEGER, SMALLINT, BIGINT, FLOAT, NUMERIC,
    VARCHAR, CHAR, TEXT, DATE, DATETIME, TIME,
    BOOLEAN, BLOB, VARBINARY
)
from spinta.datasets.backends.sql.backends.sas.types import (
    SASDateType, SASDateTimeType, SASTimeType, SASStringType
)


def map_sas_type_to_sqlalchemy(col_type: str, length: Any = None, format_str: Optional[str] = None, cache: Optional[Dict] = None) -> sa.types.TypeEngine:
    """
    Map SAS data types to SQLAlchemy types.

    SAS has two main data types:
    - 'num': Numeric (floats, integers, dates, times)
    - 'char': Character strings

    The specific interpretation depends on the format attached to the column.

    Args:
        col_type: SAS column type ('num' or 'char')
        length: Column length in bytes
        format_str: SAS format string (e.g., 'DATE9.', 'DATETIME20.')
        cache: Optional dictionary for caching type mappings

    Returns:
        SQLAlchemy type instance
    """
    # Create cache key
    cache_key = (col_type, length, format_str)

    # Check cache if provided
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    col_type = col_type.lower()
    format_str = format_str.upper() if format_str else ""

    sa_type = None

    if col_type == 'char':
        # Default for character types
        if format_str.startswith('$HEX'):
            sa_type = VARBINARY
        elif format_str == '$GEOREF':
             # Use generic TypeEngine if GeoAlchemy2 not available or for simple mapping
             # In a real implementation we would conditionally import Geometry
            sa_type = TEXT  # Fallback to TEXT if not geometry
        else:
            try:
                length_int = int(length) if length else 255
            except (ValueError, TypeError):
                length_int = 255
            sa_type = SASStringType(length=length_int)

    elif col_type == 'num':
        # Default is FLOAT/NUMERIC unless format indicates date/time
        if any(fmt in format_str for fmt in ['DATE', 'YEAR', 'MMDDYY', 'DDMMYY', 'E8601DA']):
            sa_type = SASDateType()
        elif any(fmt in format_str for fmt in ['DATETIME', 'E8601DT']):
            sa_type = SASDateTimeType()
        elif any(fmt in format_str for fmt in ['TIME', 'E8601TM']):
            sa_type = SASTimeType()
        elif any(fmt in format_str for fmt in ['DOLLAR', 'NLMNY']):
            sa_type = NUMERIC
        else:
            # SAS stores all numbers as floating point doubles (8 bytes)
            # Short lengths might imply integers
            try:
                length_int = int(length) if length else 8
            except (ValueError, TypeError):
                length_int = 8

            if length_int < 8 and '.' not in format_str:
                sa_type = INTEGER
            else:
                sa_type = FLOAT

    else:
        # Fallback
        sa_type = TEXT

    # Update cache if provided
    if cache is not None:
        cache[cache_key] = sa_type

    return sa_type

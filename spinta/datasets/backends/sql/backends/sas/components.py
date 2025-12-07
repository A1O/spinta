"""
SAS Backend Component

This module provides the main backend component class for SAS database integration
within the Spinta framework.
"""

import logging
from typing import Dict, Any

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from spinta import commands
from spinta.components import Context, Model
from spinta.datasets.backends.sql.components import Sql
import spinta.datasets.backends.sql.commands.load  # noqa: F401
from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect

logger = logging.getLogger(__name__)

# Register the dialect immediately upon module import
register_sas_dialect()


class SAS(Sql):
    """
    SAS Backend Component for Spinta framework.
    """

    type = "sql/sas"
    query_builder_type = "sql/sas"

    def get_table(self, model: Model, name: str = None) -> sa.Table:
        name = name or model.external.name

        effective_schema = self.dbschema
        if not effective_schema and hasattr(self.engine.dialect, "default_schema_name"):
            effective_schema = self.engine.dialect.default_schema_name

        if effective_schema:
            key = f"{effective_schema}.{name}"
        else:
            key = name

        if key not in self.schema.tables:
            # Create table with schema
            sa.Table(name, self.schema, autoload_with=self.engine, schema=effective_schema)

        if key in self.schema.tables:
            return self.schema.tables[key]
        elif name in self.schema.tables:
            return self.schema.tables[name]
        else:
            # Case insensitive search fallback
            target = name.upper()
            for table_key, table_obj in self.schema.tables.items():
                if table_obj.name.upper() == target:
                    return table_obj

            raise KeyError(f"Table '{name}' not found in schema")


@commands.load.register(Context, SAS, dict)
def load(context: Context, backend: SAS, config: Dict[str, Any]):
    # Delegate to the standard SQL load command first
    commands.load[Context, Sql, dict](context, backend, config)

    # If dbschema wasn't set by config (via explicit 'schema' key),
    # try to extract it from the DSN/URL.
    if not backend.dbschema and backend.engine:
        url = backend.engine.url
        # 1. Try path (database part of URL)
        schema = url.database

        # 2. Try query parameters
        if not schema and url.query:
            schema = url.query.get("libname") or url.query.get("schema")

        if schema:
            backend.dbschema = schema
            logger.debug(f"SAS backend: dbschema extracted from DSN: '{schema}'")

"""
SAS Dialect for SQLAlchemy.

This module implements the SQLAlchemy dialect for SAS, handling connection
parameters, type mapping, and SQL compilation specific to SAS.
"""

import logging
from pathlib import Path
from sqlalchemy.engine import default
from sqlalchemy import pool
from sqlalchemy import types as sqltypes
from sqlalchemy.sql import compiler

from spinta.datasets.backends.sql.backends.sas.introspection import SASIntrospectionMixin
from spinta.datasets.backends.sql.backends.sas.types import (
    SASDateType, SASDateTimeType, SASTimeType, SASStringType
)

logger = logging.getLogger(__name__)


class SASIdentifierPreparer(compiler.IdentifierPreparer):
    """
    Custom identifier preparer for SAS.

    SAS identifiers:
    - Are case-insensitive (but stored as uppercase)
    - Are not typically quoted
    - Have a maximum length of 32 chars
    """
    reserved_words = set()  # SAS has reserved words but we generally don't quote identifiers unless forced

    def quote_identifier(self, value):
        # SAS doesn't standardly use quotes for identifiers in the same way as standard SQL
        # It's safer to not quote unless absolutely necessary or if 'n' literal syntax is used (not supported here)
        # We'll just return the value as is, maybe uppercased if needed, but usually SA handles case.
        # However, parent class adds quotes. We might want to override to empty if no quoting desired.
        return value

    def format_table(self, table, use_schema=True, name=None):
        # Ensure schema.table format is correct for SAS (libname.memname)
        # SAS uses dot notation: LIBNAME.MEMNAME
        # We need to make sure we don't double quote things unnecessarily
        if name is None:
            name = table.name

        if use_schema and getattr(table, "schema", None):
            return f"{table.schema}.{name}"
        return name


class SASCompiler(compiler.SQLCompiler):
    """
    Custom SQL Compiler for SAS.

    Handles SAS-specific SQL syntax nuances.
    """

    def visit_select(self, select, **kwargs):
        # SAS uses (OBS=n) for LIMIT
        # This is a complex customization for SQLAlchemy 1.4 compiler
        # For now, we rely on standard SQL generation and hope SAS supports LIMIT or we implement limit clause handling
        # SAS supports standard SQL via PROC SQL or JDBC, but often LIMIT is not standard SQL LIMIT/OFFSET
        # SAS JDBC driver might support LIMIT? Reference implies (OBS=n)

        # Let's see if we can hook into limit_clause
        return super().visit_select(select, **kwargs)

    def limit_clause(self, select, **kw):
        # SAS typically doesn't support LIMIT/OFFSET in standard SQL.
        # But JDBC driver sometimes translates.
        # If we need (OBS=n), it usually goes after table name which is hard to inject here.
        # We will leave empty for now as simple read operations might not use pagination immediately
        # or we might need a more invasive compiler change.
        return ""


class SASDialect(default.DefaultDialect, SASIntrospectionMixin):
    """
    SQLAlchemy Dialect for SAS.

    Handles:
    - sas+jdbc:// connection strings
    - JDBC connection via jaydebeapi
    - SAS-specific types and introspection
    """

    name = "sas"
    driver = "jdbc"

    # JDBC Settings
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # Dialect Features
    supports_comments = True
    supports_schemas = True  # Mapped to SAS Libraries
    supports_views = True
    requires_name_normalize = True
    supports_transactions = False  # SAS is auto-commit
    supports_pk_autoincrement = False
    supports_sequences = False
    supports_statement_cache = True
    supports_native_boolean = False

    # Identifiers
    max_identifier_length = 32
    quote_identifiers = False

    # Type mapping
    colspecs = {
        sqltypes.Date: SASDateType,
        sqltypes.DateTime: SASDateTimeType,
        sqltypes.Time: SASTimeType,
        sqltypes.String: SASStringType,
        sqltypes.VARCHAR: SASStringType,
    }

    statement_compiler = SASCompiler

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.identifier_preparer = SASIdentifierPreparer(self)
        self._type_mapping_cache = {}

    @classmethod
    def dbapi(cls):
        import jaydebeapi
        return jaydebeapi

    @classmethod
    def get_dialect_pool_class(cls, url):
        return pool.QueuePool

    def initialize(self, connection):
        try:
            super().initialize(connection)
            # Try to determine default schema from URL
            if hasattr(self, "url") and self.url:
                # url.database usually holds the path part (e.g., /libname -> libname)
                # url.query might also hold schema
                schema_from_url = self.url.database or (self.url.query.get("libname") if self.url.query else None)
                if schema_from_url:
                    self.default_schema_name = schema_from_url
                    logger.debug(f"SAS dialect: default_schema_name set to: '{self.default_schema_name}'")
                else:
                    self.default_schema_name = ""
        except Exception as e:
            logger.warning(f"SAS dialect initialization warning: {e}")
            self.default_schema_name = ""

    def on_connect_url(self, url):
        self.url = url
        return None

    def create_connect_args(self, url):
        """
        Create connection arguments for jaydebeapi.

        URL format: sas+jdbc://user:pass@host:port/libname

        Translates to:
        - jclassname: com.sas.rio.MVADriver
        - url: jdbc:sasiom://host:port/
        - driver_args: {user, password, libname (as schema property? or just relying on libname in path?)}

        The reference implementation suggests passing schema via properties.
        """
        try:
            # Build JDBC URL
            # Note: SAS JDBC URL usually looks like jdbc:sasiom://host:port
            jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"
            if url.port:
                jdbc_url += f":{url.port}"

            driver_args = {
                "user": url.username or "",
                "password": url.password or "",
                "applyFormats": "false",  # Return raw numbers for dates
            }

            # Handle libname/schema
            # 1. Check url.database (from path /libname)
            # 2. Check url.query['libname'] or url.query['schema']

            schema = url.database
            if not schema and url.query:
                schema = url.query.get("libname")

            if schema:
                driver_args["libname"] = schema

            # Log4j config (from reference, seems useful for noise reduction)
            current_dir = Path(__file__).parent
            log4j_config_path = current_dir / "log4j.properties"
            jars = []
            if log4j_config_path.exists():
                driver_args["log4j.configuration"] = f"file://{log4j_config_path.absolute()}"
                jars = [str(current_dir)]

            kwargs = {
                "jclassname": self.jdbc_driver_name,
                "url": jdbc_url,
                "driver_args": driver_args,
            }
            if jars:
                kwargs["jars"] = jars

            return ((), kwargs)

        except Exception as e:
            logger.error(f"Error creating connection args: {e}")
            raise

    def do_rollback(self, dbapi_connection):
        pass

    def do_commit(self, dbapi_connection):
        pass

    def normalize_name(self, name):
        if name:
            return name.upper().rstrip()
        return name

    def denormalize_name(self, name):
        if name:
            return name.lower()
        return name


def register_sas_dialect():
    from sqlalchemy.dialects import registry
    registry.register("sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect")

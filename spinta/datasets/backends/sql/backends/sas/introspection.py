"""
SAS Database Introspection Logic.

This module provides methods to inspect SAS library metadata using
dictionary tables (DICTIONARY.TABLES, DICTIONARY.COLUMNS, etc.).
"""

import logging
from spinta.datasets.backends.sql.backends.sas.helpers import map_sas_type_to_sqlalchemy

logger = logging.getLogger(__name__)


class SASIntrospectionMixin:
    """
    Mixin class that provides SAS introspection capabilities to the dialect.
    """

    def _safe_value_to_str(self, value):
        """Helper to convert potentially byte strings to normal strings."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    def _escape_param(self, value: str) -> str:
        """
        Escapes a parameter value for use in a SAS SQL string literal.
        Replaces single quotes with two single quotes.
        """
        if value is None:
            return ''
        return str(value).replace("'", "''")

    def get_schema_names(self, connection, **kw):
        """
        Retrieve list of schema (library) names.
        """
        try:
            query = "SELECT DISTINCT libname FROM dictionary.libnames ORDER BY libname"
            result = connection.execute(query)
            return [self._safe_value_to_str(row[0]) for row in result]
        except Exception as e:
            logger.error(f"Failed to retrieve schema names: {e}")
            return []

    def get_table_names(self, connection, schema=None, **kw):
        """
        Retrieve list of table names in a schema.
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            # Use interpolated string with escaping instead of parameters
            # to work around potential JDBC driver issues with params in introspection queries
            libname_val = self._escape_param(schema.upper() if schema else '')

            query = f"""
            SELECT memname
            FROM dictionary.tables
            WHERE libname = '{libname_val}' AND memtype = 'DATA'
            ORDER BY memname
            """

            result = connection.execute(query)
            return [self._safe_value_to_str(row[0]).strip() for row in result]
        except Exception as e:
            logger.error(f"Failed to retrieve table names for schema {schema}: {e}")
            return []

    def get_view_names(self, connection, schema=None, **kw):
        """
        Retrieve list of view names from a schema.
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            libname_val = self._escape_param(schema.upper() if schema else '')

            query = f"""
            SELECT memname
            FROM dictionary.tables
            WHERE libname = '{libname_val}' AND memtype = 'VIEW'
            ORDER BY memname
            """
            result = connection.execute(query)
            return [self._safe_value_to_str(row[0]).strip() for row in result]
        except Exception as e:
            logger.error(f"Failed to retrieve view names for schema {schema}: {e}")
            return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Retrieve column metadata for a table.
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            libname_val = self._escape_param(schema.upper() if schema else '')
            memname_val = self._escape_param(table_name.upper())

            query = f"""
            SELECT
                name,
                type,
                length,
                format,
                label,
                notnull
            FROM dictionary.columns
            WHERE libname = '{libname_val}' AND memname = '{memname_val}'
            ORDER BY varnum
            """

            result = connection.execute(query)

            columns = []
            for row in result:
                col_name = self._safe_value_to_str(row[0])
                col_type = self._safe_value_to_str(row[1])  # 'num' or 'char'
                col_length = self._safe_value_to_str(row[2])
                col_format = self._safe_value_to_str(row[3])
                col_label = self._safe_value_to_str(row[4])
                # notnull can be numeric (0 or 1), convert to bool directly
                col_notnull = bool(row[5]) if row[5] is not None else False

                # Map SAS type to SQLAlchemy type
                sa_type = map_sas_type_to_sqlalchemy(col_type, col_length, col_format, self._type_mapping_cache)

                column_info = {
                    "name": col_name,
                    "type": sa_type,
                    "nullable": not bool(col_notnull),
                    "default": None,
                }

                if col_label:
                    column_info["comment"] = col_label

                columns.append(column_info)

            return columns
        except Exception as e:
            logger.error(f"Failed to retrieve columns for table {table_name}: {e}")
            return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        try:
            if schema is None:
                schema = self.default_schema_name

            libname_val = self._escape_param(schema.upper() if schema else '')
            memname_val = self._escape_param(table_name.upper())

            query = f"""
            SELECT
                indxname,
                name,
                unique
            FROM dictionary.indexes
            WHERE libname = '{libname_val}' AND memname = '{memname_val}'
            ORDER BY indxname, indxpos
            """

            result = connection.execute(query)

            indexes = {}
            for row in result:
                idx_name = self._safe_value_to_str(row[0])
                col_name = self._safe_value_to_str(row[1])
                is_unique = bool(row[2]) if row[2] is not None else False

                if idx_name not in indexes:
                    indexes[idx_name] = {"name": idx_name, "column_names": [], "unique": is_unique}

                indexes[idx_name]["column_names"].append(col_name)

            return list(indexes.values())
        except Exception as e:
            logger.error(f"Failed to retrieve indexes for table {table_name}: {e}")
            return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            if schema is None:
                schema = self.default_schema_name

            libname_val = self._escape_param(schema.upper() if schema else '')
            memname_val = self._escape_param(table_name.upper())

            query = f"""
            SELECT memlabel
            FROM dictionary.tables
            WHERE libname = '{libname_val}' AND memname = '{memname_val}'
            """

            result = connection.execute(query)
            row = result.fetchone()
            if row and row[0]:
                return {"text": self._safe_value_to_str(row[0]).strip()}
            return {"text": None}
        except Exception as e:
            logger.error(f"Failed to retrieve table comment for {table_name}: {e}")
            return {"text": None}

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            if schema is None:
                schema = self.default_schema_name

            libname_val = self._escape_param(schema.upper() if schema else '')
            memname_val = self._escape_param(table_name.upper())

            query = f"""
            SELECT COUNT(*)
            FROM dictionary.tables
            WHERE libname = '{libname_val}' AND memname = '{memname_val}' AND memtype = 'DATA'
            """
            result = connection.execute(query)
            row = result.fetchone()
            return int(row[0]) > 0 if row else False
        except Exception as e:
            logger.error(f"Failed to check table existence for {table_name}: {e}")
            return False

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        return False

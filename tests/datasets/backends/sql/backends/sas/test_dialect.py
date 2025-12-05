from unittest.mock import MagicMock, patch
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from spinta.datasets.backends.sql.backends.sas.dialect import SASDialect, register_sas_dialect

def test_create_connect_args_path_libname():
    dialect = SASDialect()
    url = make_url("sas+jdbc://user:pass@host:1234/mylib")

    args, kwargs = dialect.create_connect_args(url)

    assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
    assert kwargs["url"] == "jdbc:sasiom://host:1234"
    assert kwargs["driver_args"]["user"] == "user"
    assert kwargs["driver_args"]["password"] == "pass"
    assert kwargs["driver_args"]["libname"] == "mylib"

def test_create_connect_args_query_libname():
    dialect = SASDialect()
    url = make_url("sas+jdbc://user:pass@host:1234/?libname=mylib")

    args, kwargs = dialect.create_connect_args(url)

    assert kwargs["driver_args"]["libname"] == "mylib"

def test_sas_types_compilation():
    # Register the dialect
    register_sas_dialect()

    # Mock jaydebeapi to avoid import error during engine creation if not installed (though we installed it)
    # But more importantly, we don't have a real DB, so we mock the connection

    with patch("jaydebeapi.connect") as mock_connect:
        engine = sa.create_engine("sas+jdbc://u:p@h/l")

        # Verify dialect loaded
        assert isinstance(engine.dialect, SASDialect)

        # Test basic compilation
        t = sa.Table("mytable", sa.MetaData(), sa.Column("id", sa.Integer), schema="lib")
        stmt = sa.select([t.c.id])
        compiled = stmt.compile(dialect=engine.dialect)

        # Check that it compiles to something reasonable
        # SAS uses schema.table syntax
        sql = str(compiled)
        assert "lib.mytable" in sql or "lib.mytable" in sql.lower() # Depends on quoting/case

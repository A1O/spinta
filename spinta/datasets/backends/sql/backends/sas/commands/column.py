from spinta import commands
from spinta.components import Property
from spinta.datasets.backends.sql.backends.sas.components import SAS
from spinta.exceptions import NoExternalName, PropertyNotFound
from spinta.types.datatype import DataType, Ref
import sqlalchemy as sa


def _get_case_insensitive_column(table: sa.Table, name: str, prop: Property):
    # First try exact match
    if name in table.c:
        return table.c[name]

    # Try case-insensitive match
    name_upper = name.upper()
    for column in table.c:
        if column.name.upper() == name_upper:
            return column

    # Not found
    raise PropertyNotFound(
        prop.model,
        property=prop.name,
        external=name,
    )


@commands.get_column.register(SAS, DataType)
def get_column(backend: SAS, dtype: DataType, table: sa.Table, **kwargs):
    prop = dtype.prop
    if not prop.external or not prop.external.name:
        raise NoExternalName(prop)

    return _get_case_insensitive_column(table, prop.external.name, prop)


@commands.get_column.register(SAS, Ref)
def get_column(backend: SAS, dtype: Ref, table: sa.Table, **kwargs):
    prop = dtype.prop
    if not prop.external or not prop.external.name and not dtype.inherited:
        raise NoExternalName(prop)

    if prop.external.name:
        return _get_case_insensitive_column(table, prop.external.name, prop)

    # Fallback or other logic if name is missing but inherited?
    # The original implementation returns None or raises error if not inherited.
    # If not prop.external.name, the first check raises NoExternalName unless inherited.
    # If it is inherited, and no name, what does the base implementation do?
    # Base implementation:
    # if prop.external.name:
    #     if prop.external.name not in table.c:
    #         raise PropertyNotFound(...)
    #     return table.c[prop.external.name]
    # return None implicitly?

    # Let's double check the base implementation behavior for Ref.
    return None


from _metadata import getMetadata

from sqlalchemy import inspect


def validate(engine):
    """Checks that the database referred to by `engine` against the pprofit
    database schema.

    Args:
        engine (sqlalchemy.Engine): Database engine for `fitting_run.db`

    Returns:
        bool: True if database is valid or False otherwise.
    """
    md = getMetadata()
    tablenames = set([table.name for table in md.sorted_tables])

    inspector = inspect(engine)

    actual_table_names = inspector.get_sorted_table_and_fkc_names()
    actual_table_names = [name for (name, fk) in actual_table_names]

    if actual_table_names == [None]:
      return False

    for tn in actual_table_names:
      if tn and not tn in tablenames:
        print tn
        return False
    return True

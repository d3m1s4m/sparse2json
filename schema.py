from sqlalchemy import inspect
from geoalchemy2 import Geometry


def get_tables_and_columns(engine):
    """Get a dictionary of all tables and their columns in the database."""
    inspector = inspect(engine)
    db_info = {}

    tables = inspector.get_table_names()
    for table in tables:
        columns = inspector.get_columns(table)
        columns_name = []
        for column in columns:
            # Check for geometry type and handle it
            if column['type'] == Geometry:
                columns_name.append(column['name'])
            else:
                columns_name.append(column["name"])

        db_info[table] = columns_name

    return db_info

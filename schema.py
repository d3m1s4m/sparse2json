from sqlalchemy import inspect


def get_tables_and_columns(engine):
    """Get a dictionary of all tables and their columns in the database."""
    inspector = inspect(engine)
    db_info = {}

    tables = inspector.get_table_names()
    for table in tables:
        columns = inspector.get_columns(table)
        columns_name = [
            column["name"] for column in columns
        ]
        db_info[table] = columns_name

    return db_info

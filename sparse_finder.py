import logging
from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from schema import get_tables_and_columns, get_columns_names


# Get the logger configured in main.py
logger = logging.getLogger('sparse2json_logger')


def find_sparse_columns(engine, table, min_rows_for_sparse_check=30, sparse_percent=40, session=None):
    """Identify sparse columns in a given table."""
    new_session_flag = False
    # If no session was given, initialize one
    if session is None:
        new_session_flag = True
        session = sessionmaker(bind=engine)()

    # Dictionary to store tables and their sparse columns
    table_sparse_columns = defaultdict(list)

    total_rows = session.execute(text(f'SELECT COUNT(*) FROM {table}')).scalar()

    # Skip tables with fewer than MIN_ROW rows
    if total_rows < min_rows_for_sparse_check:
        return {}

    columns = get_columns_names(engine, table)
    for column in columns:
        # Wrap column names in double quotes to handle special characters
        column_name = f'"{column}"'
        try:
            null_count = session.execute(
                text(f'SELECT COUNT(*) FROM {table} WHERE {column_name} IS NULL')
            ).scalar()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

            # If more than 'sparce_percent'% of the column values are NULL, consider it sparse
            if null_percentage > sparse_percent:
                table_sparse_columns[table].append(column)

        except Exception as e:
            logger.error(f"Error processing column '{column}' in table '{table}': {e}")

    if new_session_flag:
        session.close()

    # If table sparse columns count is less than 2 return {}
    return table_sparse_columns if len(table_sparse_columns[table]) > 1 else {}


def find_sparse_tables(engine, min_rows_for_sparse_check=30, sparse_percent=40):
    """Finds tables that have more than one sparse column"""
    # Create a session for querying the database
    session = sessionmaker(bind=engine)()

    # Dictionary to store tables with more than one sparse column
    tables_to_fix = defaultdict(list)

    # Get all tables and their columns
    db_info = get_tables_and_columns(engine)

    tables = db_info.keys()
    for table in tables:
        table_sparse_columns = find_sparse_columns(engine, table, min_rows_for_sparse_check, sparse_percent, session)

        # Only add non-empty results to the main dictionary
        if table_sparse_columns:
            for table_name, sparse_columns in table_sparse_columns.items():
                tables_to_fix[table_name].extend(sparse_columns)

    session.close()

    return tables_to_fix
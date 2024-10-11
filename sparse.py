from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from schema import get_tables_and_columns


MIN_ROWS_FOR_SPARSE_CHECK = 30


def find_sparse(engine):
    """Finds tables that have more than one sparse column"""
    # Create a session for querying the database
    session = sessionmaker(bind=engine)()

    # Dictionary to store tables and their sparse columns
    sparse_columns = defaultdict(list)

    # Get all tables and their columns
    db_info = get_tables_and_columns(engine)

    tables = db_info.keys()
    for table in tables:
        total_rows = session.execute(text(f'SELECT COUNT(*) FROM {table}')).scalar()

        # Skip tables with fewer than MIN_ROW rows
        if total_rows < MIN_ROWS_FOR_SPARSE_CHECK:
            continue

        for column in db_info[table]:
            try:
                null_count = session.execute(
                    text(f'SELECT COUNT(*) FROM {table} WHERE {column} IS NULL')
                ).scalar()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

                # If more than 40% of the column values are NULL, consider it sparse
                if null_percentage > 40:
                    sparse_columns[table].append(column)

            except Exception as e:
                print(f"Error processing column '{column}' in table '{table}': {e}")

    # Filter out tables with more than one sparse column
    tables_to_fix = {table: cols for table, cols in sparse_columns.items() if len(cols) > 1}

    return tables_to_fix

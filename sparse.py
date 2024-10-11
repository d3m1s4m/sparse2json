import logging
from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from schema import get_tables_and_columns


MIN_ROWS_FOR_SPARSE_CHECK = 30

# Configure logging
logging.basicConfig(
    filename='sparse.log',  # Log file name
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log message format
)


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

        columns = db_info[table]
        for column in columns:
            # Wrap column names in double quotes to handle special characters
            column_name = f'"{column}"'
            try:
                null_count = session.execute(
                    text(f'SELECT COUNT(*) FROM {table} WHERE {column_name} IS NULL')
                ).scalar()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

                # If more than 40% of the column values are NULL, consider it sparse
                if null_percentage > 40:
                    sparse_columns[table].append(column)

            except Exception as e:
                logging.error(f"Error processing column '{column}' in table '{table}': {e}")

    # Filter out tables with more than one sparse column
    tables_to_fix = {table: cols for table, cols in sparse_columns.items() if len(cols) > 1}

    session.close()  # Close the session after completing the operation
    return tables_to_fix


def convert_sparse_to_json(engine, tables_to_fix):
    """Convert sparse columns to a JSON field for tables that need fixing."""
    # Create a session for querying the database
    session = sessionmaker(bind=engine)()

    for table, sparse_columns in tables_to_fix.items():
        logging.info(f"Converting sparse columns for table: {table}")

        # Add a new JSONB column to the table
        json_column = 'sparse_data'
        try:
            session.execute(text(f"ALTER TABLE {table} ADD COLUMN {json_column} JSONB"))
            session.commit()
            logging.info(f"Added JSON column '{json_column}' to table '{table}'.")
        except Exception as e:
            logging.error(f"Failed to add JSON column to {table}: {e}")
            session.rollback()
            continue

        # Migrate sparse columns data into the JSON column
        for sparse_column in sparse_columns:
            try:
                # Quote the sparse column name to handle special characters
                quoted_column = f'"{sparse_column}"'
                # Updates the JSONB column {json_column} by:
                # 1. Using COALESCE to initialize it as an empty JSON object if it is NULL (has not been populated yet).
                # 2. Adding a new key-value pair from the sparse column {sparse_column}.
                # Only updates rows where {quoted_column} is NOT NULL.
                session.execute(text(f"""
                    UPDATE {table} 
                    SET {json_column} = COALESCE({json_column}, '{{}}'::jsonb) 
                    || jsonb_build_object('{sparse_column}', {quoted_column})
                    WHERE {quoted_column} IS NOT NULL
                """))
                session.commit()
                logging.info(f"Moved data from column '{sparse_column}' to JSON field in table '{table}'.")
            except Exception as e:
                logging.error(f"Failed to migrate column '{sparse_column}' data for table '{table}': {e}")
                session.rollback()

        # Drop the original sparse columns
        for sparse_column in sparse_columns:
            try:
                # Quote the sparse column name to handle reserved keywords
                quoted_column = f'"{sparse_column}"'
                session.execute(text(f"ALTER TABLE {table} DROP COLUMN {quoted_column}"))
                session.commit()
                logging.info(f"Dropped column '{sparse_column}' from table '{table}'.")
            except Exception as e:
                logging.error(f"Failed to drop column '{sparse_column}' from table '{table}': {e}")
                session.rollback()

    session.close()  # Close the session after completing the operation

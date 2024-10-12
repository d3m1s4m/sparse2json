import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


# Get the logger configured in main.py
logger = logging.getLogger('sparse2json_logger')


def convert_sparse_to_json(engine, tables_to_fix, is_verbose):
    """Convert sparse columns to a JSON field for tables that need fixing."""
    # Create a session for querying the database
    session = sessionmaker(bind=engine)()

    for table, sparse_columns in tables_to_fix.items():
        logger.info(f"Converting sparse columns for table: {table}")
        if is_verbose:
            print(f"Converting sparse columns for table: {table}")

        # Define JSON column name
        json_column = 'sparse_data'

        # Add a new JSONB column to the table
        add_json_column(session, table, json_column)

        # Migrate sparse columns data into the JSON column
        migrate_sparse_to_json(session, table, sparse_columns, json_column)

        # Drop the original sparse columns
        drop_original_sparse_columns(session, table, sparse_columns)

    session.close()


def add_json_column(session, table, json_column):
    """Add a new JSONB column to the table."""
    try:
        session.execute(text(f"ALTER TABLE {table} ADD COLUMN {json_column} JSONB"))
        session.commit()
        logger.info(f"Added JSON column '{json_column}' to table '{table}'.")
    except Exception as e:
        logger.error(f"Failed to add JSON column to {table}: {e}")
        session.rollback()
        return


def migrate_sparse_to_json(session, table, sparse_columns, json_column):
    """Migrate sparse columns data into the JSON column."""
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
            logger.info(f"Moved data from column '{sparse_column}' to JSON field in table '{table}'.")
        except Exception as e:
            logger.error(f"Failed to migrate column '{sparse_column}' data for table '{table}': {e}")
            session.rollback()


def drop_original_sparse_columns(session, table, sparse_columns):
    """Drop the original sparse columns"""
    for sparse_column in sparse_columns:
        try:
            # Quote the sparse column name to handle reserved keywords
            quoted_column = f'"{sparse_column}"'
            session.execute(text(f"ALTER TABLE {table} DROP COLUMN {quoted_column}"))
            session.commit()
            logger.info(f"Dropped column '{sparse_column}' from table '{table}'.")
        except Exception as e:
            logger.error(f"Failed to drop column '{sparse_column}' from table '{table}': {e}")
            session.rollback()

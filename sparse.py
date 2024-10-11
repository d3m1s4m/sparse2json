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
                print(f"Error processing column '{column}' in table '{table}': {e}")

    # Filter out tables with more than one sparse column
    tables_to_fix = {table: cols for table, cols in sparse_columns.items() if len(cols) > 1}

    return tables_to_fix


def convert_sparse_to_json(engine, tables_to_fix):
    """Convert sparse columns to a JSON field for tables that need fixing."""
    # Create a session for querying the database
    session = sessionmaker(bind=engine)()

    for table, sparse_columns in tables_to_fix.items():
        print(f"Converting sparse columns for table: {table}")

        # Add a new JSONB column to the table
        json_column = 'sparse_data'
        try:
            session.execute(text(f"ALTER TABLE {table} ADD COLUMN {json_column} JSONB"))
            session.commit()
            print(f"Added JSON column '{json_column}' to table '{table}'.")
        except Exception as e:
            print(f"Failed to add JSON column to {table}: {e}")
            session.rollback()
            continue

        # Migrate sparse columns data into the JSON column
        for sparse_column in sparse_columns:
            try:
                session.execute(text(f"""
                    UPDATE {table} 
                    SET {json_column} = COALESCE({json_column}, '{{}}'::jsonb) 
                    || jsonb_build_object('{sparse_column}', {sparse_column})
                    WHERE {sparse_column} IS NOT NULL
                """))
                session.commit()
                print(f"Moved data from column '{sparse_column}' to JSON field in table '{table}'.")
            except Exception as e:
                print(f"Failed to migrate column '{sparse_column}' data for table '{table}': {e}")
                session.rollback()

        # Drop the original sparse columns
        for sparse_column in sparse_columns:
            try:
                session.execute(text(f"ALTER TABLE {table} DROP COLUMN {sparse_column}"))
                session.commit()
                print(f"Dropped column '{sparse_column}' from table '{table}'.")
            except Exception as e:
                print(f"Failed to drop column '{sparse_column}' from table '{table}': {e}")
                session.rollback()

import logging.config
from cli import parse_arguments, get_db_password, get_min_rows_for_sparse_check, get_sparse_percent
from config import LOGGING_CONFIG
from db import create_db_engine
from schema import check_table_exists
from sparse_finder import find_sparse_tables, find_sparse_columns
from sparse_convertor import convert_sparse_to_json


# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)

# Parse arguments
args = parse_arguments()


def run():
    # Receive the database password from user
    password = get_db_password()

    # Create the database engine
    engine = create_db_engine(args.host, args.user, password, args.dbname, args.port)

    if args.table and not check_table_exists(engine, args.table):
        print("Given table name doesn't exists")
        return

    # Receive min rows for sparse check from user
    min_rows_for_sparse_check = get_min_rows_for_sparse_check()

    # Receive sparse level from user
    sparse_percent = get_sparse_percent()

    print('Running. This may take some time.')

    # Find tables to fix
    if args.table:
        tables_to_fix = find_sparse_columns(engine, args.table, min_rows_for_sparse_check, sparse_percent)
        print(tables_to_fix)
    else:
        tables_to_fix = find_sparse_tables(engine, min_rows_for_sparse_check, sparse_percent)

    # Convert sparse columns to JSON for the identified tables
    convert_sparse_to_json(engine, tables_to_fix, is_verbose=args.verbose)

    # Check if any tables need fixing
    if not bool(tables_to_fix):
        print('No tables found to fix with the given condition.')
    else:
        print('Done!')


if __name__ == '__main__':
    run()

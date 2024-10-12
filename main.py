import argparse
import logging.config
from config import LOGGING_CONFIG
from db import create_db_engine
from schema import check_table_exists
from sparse_finder import find_sparse_tables, find_sparse_columns
from sparse_convertor import convert_sparse_to_json


# Command-line argument parsing
parser = argparse.ArgumentParser(description='Sparse2JSON')
parser.add_argument('-H', '--host', required=True, help='Database host')
parser.add_argument('-U', '--user', required=True, help='Database user')
parser.add_argument('-P', '--password', required=True, help='Database password')
parser.add_argument('-D', '--dbname', required=True, help='Database name')
parser.add_argument('-p', '--port', default=5432, help='Database port')
parser.add_argument('-T', '--table', help='Database specific table')
parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')

args = parser.parse_args()

# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)

# Create the database engine
engine = create_db_engine(args.host, args.user, args.password, args.dbname, args.port)


def get_min_rows_for_sparse_check():
    while True:
        user_input = input('Enter min rows for sparse check (default 30): ')
        if user_input.strip() == '':
            return 30
        try:
            value = int(user_input)
            if value < 0:
                print('Please enter a non-negative integer.')
            else:
                return value
        except ValueError:
            print('Invalid input. Please enter a valid integer.')


def get_sparse_percent():
    levels = {1: 30, 2: 40, 3: 50, 4: 60, 5: 70}

    print('Select sparse level (default 2):')
    for level, percentage in levels.items():
        print(f'{level}: More than {percentage}% NULL values')

    user_input = input('Enter the sparse level (1-5): ')
    if user_input.strip() == '':
        return levels[2]

    try:
        level = int(user_input)
        if level in levels:
            return levels[level]
        else:
            print("Invalid level. Please enter a number between 1 and 5.")
            return get_sparse_percent()
    except ValueError:
        print("Invalid input. Please enter a valid integer.")
        return get_sparse_percent()


def run():
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

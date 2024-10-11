import argparse
from db import create_db_engine
from sparse import find_sparse, convert_sparse_to_json
import logging.config
from config import LOGGING_CONFIG


# Command-line argument parsing
parser = argparse.ArgumentParser(description='Database configuration')
parser.add_argument('-H', '--host', required=True, help='Database host')
parser.add_argument('-P', '--port', required=True, help='Database port')
parser.add_argument('-U', '--user', required=True, help='Database user')
parser.add_argument('-P', '--password', required=True, help='Database password')
parser.add_argument('-D', '--dbname', required=True, help='Database name')

args = parser.parse_args()


# Create the database engine
engine = create_db_engine(args.host, args.user, args.password, args.dbname)


# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)


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


def run():
    # Receive min rows for sparse check from user
    min_rows_for_sparse_check = get_min_rows_for_sparse_check()

    # Find tables to fix
    tables_to_fix = find_sparse(engine, min_rows_for_sparse_check)

    # Convert sparse columns to JSON for the identified tables
    convert_sparse_to_json(engine, tables_to_fix)
    print('Sparse columns converted to JSON successfully.')

    # Check if any tables need fixing
    if not bool(tables_to_fix):
        print('No tables found to fix with the given condition.')
        return


if __name__ == '__main__':
    run()

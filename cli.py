import argparse
import getpass


def parse_arguments():
    """Command-line argument parsing."""
    parser = argparse.ArgumentParser(description='Sparse2JSON')
    parser.add_argument('-H', '--host', required=True, help='Database host')
    parser.add_argument('-P', '--port', default=5432, help='Database port')
    parser.add_argument('-U', '--user', required=True, help='Database user')
    # parser.add_argument('-p', '--password', required=True, help='Database password')
    parser.add_argument('-D', '--dbname', required=True, help='Database name')
    parser.add_argument('-T', '--table', help='Database specific table')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')

    return parser.parse_args()


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


def get_db_password():
    password = getpass.getpass('Enter your password: ')
    return password

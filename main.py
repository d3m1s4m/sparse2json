import argparse
from db import create_db_engine


# Command-line argument parsing
parser = argparse.ArgumentParser(description='Database configuration')
parser.add_argument('-H', '--host', required=True, help='Database host')
parser.add_argument('-U', '--user', required=True, help='Database username')
parser.add_argument('-P', '--password', required=True, help='Database password')
parser.add_argument('-D', '--dbname', required=True, help='Database name')

args = parser.parse_args()


# Create the database engine
engine = create_db_engine(args.host, args.user, args.password, args.dbname)


if __name__ == '__main__':
    print('Hello, World!')

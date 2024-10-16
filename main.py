import logging.config
import queue
import threading
from cli import parse_arguments, get_db_password, get_min_rows_for_sparse_check, get_sparse_percent
from config import LOGGING_CONFIG
from db import create_db_engine
from schema import check_table_exists
from sparse_finder import find_sparse_tables, find_sparse_columns
from sparse_convertor import convert_sparse_to_json


# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('sparse2json_logger')

# Parse arguments
args = parse_arguments()


def worker(q, engine, is_verbose):
    while True:
        table_to_fix = q.get()

        # Break the loop when None is received
        if table_to_fix is None:
            logger.info("Worker received sentinel and is terminating.")
            q.task_done()
            break

        try:
            convert_sparse_to_json(engine, table_to_fix, is_verbose)
        except Exception as e:
            logger.error(f'Error while fixing table: {e}')
        finally:
            q.task_done()


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

        # Convert sparse columns to JSON for the identified tables
        convert_sparse_to_json(engine, tables_to_fix, is_verbose=args.verbose)
    else:
        tables_to_fix = find_sparse_tables(engine, min_rows_for_sparse_check, sparse_percent)

        tables_to_fix_count = len(tables_to_fix)

        # Check if there is more than 1 table to fix to use threading
        if tables_to_fix_count > 1:
            # Create task queue
            q = queue.Queue()

            max_threads = 5
            thread_count = min(tables_to_fix_count, max_threads)

            # Create worker threads
            threads = []
            for i in range(thread_count):
                t = threading.Thread(target=worker, args=(q, engine, args.verbose))
                threads.append(t)
                t.start()

            # Add tasks to queue
            for table, columns in tables_to_fix.items():
                q.put({table: columns})

            # Add sentinel (None) for each thread to stop them after task completion
            for _ in threads:
                q.put(None)

            # Wait for all tasks to be done
            q.join()

            # Ensure all threads terminate
            for t in threads:
                t.join()

        else:
            # Convert sparse columns to JSON for the identified tables
            convert_sparse_to_json(engine, tables_to_fix, is_verbose=args.verbose)

    # Check if any tables need fixing
    if not bool(tables_to_fix):
        print('No tables found to fix with the given condition.')
    else:
        print('Done!')


if __name__ == '__main__':
    run()

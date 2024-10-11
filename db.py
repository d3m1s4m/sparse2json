import logging
from sqlalchemy import create_engine


# Get the logger configured in main.py
logger = logging.getLogger('sparse2json_logger')


def create_db_engine(host, user, password, dbname, port=5432):
    """Create a database engine and test the connection."""
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        # Attempt to connect to the database
        with engine.connect() as connection:
            logger.info("Database connection successful.")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise ValueError(f"Error connecting to database: {e}")

from sqlalchemy import create_engine


def create_db_engine(host, user, password, dbname, port=5432):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        # Attempt to connect to the database
        with engine.connect() as connection:
            print("Database connection successful.")
        return engine
    except Exception as e:
        raise ValueError(f"Error connecting to database: {e}")

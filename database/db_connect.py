from sqlalchemy import create_engine
from log.logging_config import setup_logger

logger = setup_logger(__name__)


def db_connection(username, password, host, db_name): 
    """
    Establishes a connection to a PostgreSQL database.

    Args:
        username (str): Username for database authentication.
        password (str): Password for database authentication.
        host (str): Hostname or IP address of the PostgreSQL server.
        db_name (str): Name of the database to connect to.

    Returns:
        tuple: A tuple containing the database connection and the SQLAlchemy engine.

    Raises:
        Exception: If an error occurs during the connection process.
    """
    try: 
        logger.info("establishing connection to postgres database")
        # Construct the connection string
        conn_str = f'postgresql+psycopg2://{username}:{password}@{host}:5432/{db_name}' 
        
        # Create an SQLAlchemy engine
        engine = create_engine(conn_str, echo=True, isolation_level = "AUTOCOMMIT")
        
        # Connect to the database
        connection = engine.connect()
        
        logger.info("Database connection successful")
        return connection, engine
    
    except Exception: 
        logger.exception("error connecting to database")
        raise 

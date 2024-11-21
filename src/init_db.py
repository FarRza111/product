import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_env_variables():
    """Load environment variables from .env file."""
    load_dotenv()
    return {
        'WAREHOUSE_DB_CONNECTION': os.getenv('WAREHOUSE_DB_CONNECTION')
    }

def initialize_warehouse_tables(engine):
    """Initialize warehouse database tables."""
    try:
        with engine.begin() as conn:
            # Read SQL file
            sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'create_tables.sql')
            with open(sql_file_path, 'r') as file:
                sql_commands = file.read()
                
            # Execute SQL commands
            conn.execute(text(sql_commands))
            
        logger.info("Successfully initialized warehouse database tables")
        
    except Exception as e:
        logger.error(f"Error initializing warehouse tables: {str(e)}")
        raise

def main():
    """Initialize warehouse database."""
    try:
        # Load environment variables
        env_vars = load_env_variables()
        warehouse_db = env_vars['WAREHOUSE_DB_CONNECTION']
        
        if not warehouse_db:
            raise ValueError("Missing warehouse database connection string")
        
        # Create engine
        engine = create_engine(warehouse_db)
        
        # Initialize tables
        initialize_warehouse_tables(engine)
        
    except Exception as e:
        logger.error(f"Error in initialization process: {str(e)}")
        raise

if __name__ == "__main__":
    main()

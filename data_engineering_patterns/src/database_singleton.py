from sqlalchemy import create_engine
from typing import Optional

class DatabaseConnection:
    """
    Singleton pattern implementation for database connection.
    Ensures only one database connection instance exists throughout the application.
    """
    _instance: Optional['DatabaseConnection'] = None
    _engine = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._engine is None:
            # Example connection string - should be configured via environment variables in production
            connection_string = "sqlite:///data_warehouse.db"
            self._engine = create_engine(connection_string)

    @property
    def engine(self):
        return self._engine

    def execute_query(self, query: str):
        """Execute SQL query using the singleton connection."""
        with self._engine.connect() as connection:
            return connection.execute(query)

# Usage example
if __name__ == "__main__":
    # Both instances will be the same
    db1 = DatabaseConnection()
    db2 = DatabaseConnection()
    print(f"Same instance? {db1 is db2}")  # Will print True

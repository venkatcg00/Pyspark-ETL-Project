from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Tuple


def connect_to_database(db_path: str, db_name: str) -> Tuple[Engine, sessionmaker]:
    """
    Establish a connection to the SQLite database.

    Parameters:
    db_path (str): The path to the SQLite database directory.
    db_name (str): The name of the SQLite database file.

    Returns:
    Tuple: A tuple containing the SQLAlchemy engine and sessionmaker objects.
    """
    full_db_path = f"{db_path}/{db_name}"
    engine = create_engine(f"sqlite:///{full_db_path}")
    Session = sessionmaker(bind=engine)
    return engine, Session


def fetch_allowed_values(
    session: sessionmaker, table_name: str, source_name: str, column_name: str
) -> List[str]:
    """
    Fetch allowed values from a specific column in an SQLite table.

    Parameters:
    session (sessionmaker): The SQLAlchemy sessionmaker object.
    table_name (str): The name of the table to query.
    source_name (str): The name of the source for which values are to be retrieved.
    column_name (str): The name of the column that stores the allowed values.

    Returns:
    List[str]: A list of allowed values from the specified column.
    """
    with session() as s:
        query = text(
            f"SELECT DISTINCT {column_name} FROM {table_name} WHERE SOURCE_ID = (SELECT SOURCE_ID FROM CSD_SOURCES WHERE UPPER(SOURCE_NAME) = UPPER({source_name})) AND ACTIVE_FLAG = 1"
        )
        result = s.execute(query)
        return [str(row[0]) for row in result.fetchall()]


def close_database_connection(engine: Engine) -> None:
    """
    Close the connection to the SQLite database.

    Parameters:
    engine (Engine): The SQLAlchemy engine object.
    """
    engine.dispose()

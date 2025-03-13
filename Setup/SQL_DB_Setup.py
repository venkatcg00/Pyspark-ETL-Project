from sqlalchemy import create_engine, text
import configparser
import os


def execute_sql_script(engine, script_path):
    """
    Execute a SQL script from a file using SQLAlchemy.

    :param engine: SQLAlchemy engine object
    :param script path: Path to SQL script file
    """

    with open(script_path, "r") as file:
        sql_script = file.read()

    try:
        with engine.connect() as connection:
            for statement in sql_script.split(";"):
                if statement.strip():
                    connection.execute(text(statement))
    except Exception as e:
        print(f"An error occured while executing sript {script_path}: {e}")


def check_and_create_directory(directory_path):
    """
    Check if a directory exists and create it if it does not.

    :param directory_path: Path to the directory
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
        except OSError as e:
            print(f"An error occurred while creating the directory: {e}")


def main():
    # Get the directory where the current Python script is located
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the parent directory
    project_directory = os.path.dirname(current_directory)

    # Construct the path to the parameter file
    parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

    # Read the parameter file
    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    # Check and create db_path if it does not exist
    check_and_create_directory(config.get("PATH", "SQL_DB_PATH"))

    db_path_name = config.get("PATH", "SQL_DB_PATH") + config.get(
        "DATABASE", "SQL_DB_NAME"
    )

    # Creat the SQLAlchemy engine
    engine = create_engine(f"sqlite:///{db_path_name}")

    # Execute the DDL Script
    execute_sql_script(engine, config.get("PATH", "SQL_DDL_SCRIPT"))

    # Execute the DML Script
    execute_sql_script(engine, config.get("PATH", "SQL_DML_SCRIPT"))


if __name__ == "__main__":
    main()

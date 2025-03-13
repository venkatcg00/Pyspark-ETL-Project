import os
import argparse
import pandas as pd
import random
import csv
from typing import List
from datetime import datetime, timedelta
from allowed_values import (
    connect_to_database,
    fetch_allowed_values,
    close_database_connection,
)
import configparser


def get_latest_csv_file(folder_path: str) -> str:
    """
    Get the latest CSV file in the specified folder.

    Parameters:
    folder_path (str): The path to the folder.

    Returns:
    str: the path to the latest CSV file.
    """
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        return None

    latest_file = max(
        [os.path.join(folder_path, f) for f in csv_files], key=os.path.getmtime
    )
    return latest_file


def get_max_record_id(csv_file_path: str, column_name: str) -> int:
    """
    Get the maximum value from a specified column in CSV file.

    Parameters:
    csv_file_path (str): The path to the CSV file.
    column_name (str): The name of the column to find the maximum value.

    Returns:
    int: The maximum value in the specified column.
    """
    while csv_file_path is not None:
        df = pd.read_csv(csv_file_path, delimiter="|")
        if column_name not in df.columns:
            return 0

        max_record_id = df[column_name].max()
        return max_record_id
    else:
        return 0


def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> list[str]:
    """
    Generate a random record with specified data fields.

    Parameters:
    record_id (int): The unique record ID.
    support_categories (List[str]): List of allowed support_categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.

    Returns:
    List[str]: A list representing the generated record.
    """
    duration = random.randint(20, 600)
    work_time = random.randint(10, duration - 10)

    return [
        record_id,
        random.choice(support_categories),
        random.choice(agent_pseudo_names),
        (datetime.now() - timedelta(days=random.randint(0, 1000))).strftime(
            "%m%d%Y%H%M%S"
        ),
        random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        random.choice(["CALL", "CHAT"]),
        random.choice(customer_types),
        duration,
        work_time,
        random.choice(
            [
                "RESOLVED",
                "PENDING RESOLUTION",
                "PENDING CUSTOMER UPDATE",
                "WORK IN PROGRESS",
                "TRANSFERRED TO ANOTHER QUEUE",
            ]
        ),
        random.choice([1, 0]),
        random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        random.choice(["WORST", "BAD", "NEUTRAL", "GOOD", "BEST"]),
    ]


def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_record_id: int,
    num_records: int,
) -> List[List[str]]:
    """
    Generate a specified number of new records and optionally update existing records.

    Parameters:
    support_categories (List[str]): List of allowed support categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.
    start_record_id (int): The starting RECORD_ID for the new records.
    num_records (int): The number of new records to generate.

    Returns:
    List[List[str]]: A list containing the new and updated records.
    """
    records = []
    record_id = start_record_id

    for i in range(int(num_records)):
        record_id = record_id + 1
        new_record = generate_random_record(
            record_id, support_categories, agent_pseudo_names, customer_types
        )

        # Introduce NULL values to some fields
        if random.random() < 0.1:
            key_to_nullify = random.randint(1, 12)
            new_record[key_to_nullify] = None

        records.append(new_record)

        # Introduce updated to data
        if random.random() < 0.25 and record_id > 1:
            update_record_id = random.randint(1, record_id)
            updated_record = generate_random_record(
                update_record_id, support_categories, agent_pseudo_names, customer_types
            )
            records.append(updated_record)

    return records


def write_csv_data(csv_folder_path: str, data: List[List[str]]) -> None:
    """
    Write the generated data to a CSV file.

    Parameters:
    csv_folder_path (str): The file path where the CSV data should be saved.
    data (List[List[str]]): The data to be written to the CSV file.
    """
    header = [
        "TICKET_IDENTIFIER",
        "SUPPORT_CATEGORY",
        "AGENT_NAME",
        "DATE_OF_CALL",
        "CALL_STATUS",
        "CALL_TYPE",
        "TYPE_OF_CUSTOMER",
        "DURATION",
        "WORK_TIME",
        "TICKET_STATUS",
        "RESOLVED_IN_FIRST_CONTACT",
        "RESOLUTION_CATEGORY",
        "RATING",
    ]

    # Create a file name with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"CSV_data_{timestamp}.csv"
    file_path = os.path.join(csv_folder_path, file_name)

    # Write the data to CSV file
    with open(file_path, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file, delimiter="|")
        writer.writerow(header)
        writer.writerows(data)


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


def main() -> None:
    # Get the directory where the current Python script is located
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the parent directory
    project_directory = os.path.dirname(current_directory)

    # Construct the path to the parameter file
    parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

    # Read the parameter file
    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    # Check and create csv_folder if it does not exist
    check_and_create_directory(config.get("PATH", "CSV_FOLDER"))

    # Connect to the database
    engine, sessionmaker = connect_to_database(
        config.get("PATH", "SQL_DB_PATH"), config.get("DATABASE", "SQL_DB_NAME")
    )

    # Fetch allowed values from the database
    support_categories: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_SUPPORT_AREAS", "'AT&T'", "SUPPORT_AREA_NAME"
    )
    agent_pseudo_names: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_AGENTS", "'AT&T'", "PSEUDO_CODE"
    )
    customer_types: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_CUSTOMER_TYPES", "'AT&T'", "CUSTOMER_TYPE_NAME"
    )

    # Close the database connection
    close_database_connection(engine)

    # Get CSV Folder path and name from config
    csv_folder_path: str = config.get("PATH", "CSV_FOLDER")

    # Fetch the maximum RECORD_ID from the existing CSV file
    csv_file_path = get_latest_csv_file(csv_folder_path)
    max_record_id: int = get_max_record_id(csv_file_path, "TICKET_IDENTIFIER")

    # Generate a random number of records if no input is provided
    parser = argparse.ArgumentParser(description="Process some input.")
    parser.add_argument(
        "input", nargs="?", default=None, help="An optional input value"
    )

    args = parser.parse_args()
    num_records: int = args.input

    if num_records is None:
        num_records = random.randint(1, 1000)

    # Generate new and possibly updated records
    records: List[List[str]] = generate_and_update_records(
        support_categories,
        agent_pseudo_names,
        customer_types,
        max_record_id,
        num_records,
    )

    # Write the records to the CSV ile
    write_csv_data(csv_folder_path, records)

    # Get the latest CSV file generated
    csv_file_path = get_latest_csv_file(csv_folder_path)


if __name__ == "__main__":
    main()

import os
import sys
import random
import configparser
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from allowed_values import (
    connect_to_database,
    fetch_allowed_values,
    close_database_connection,
)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from Setup import NO_SQL_DB_Setup


def generate_random_record(
    interaction_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> Dict[str, Optional[Any]]:
    """
    Generate a random record with specified data fields.

    Parameters:
    interaction_id (int): The unique identifier of a support incident.
    support_categories (List[str]): List of allowed support_categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.

    Returns:
    Dict[str, Optional[Any]]: A dictionary representing the generated record.
    """

    interaction_duration: int = random.choice([random.randint(10, 600)])

    return {
        "INTERACTION_ID": interaction_id,
        "SUPPORT_CATEGORY": random.choice(support_categories),
        "AGENT_PSEUDO_NAME": random.choice(agent_pseudo_names),
        "CONTACT_DATE": (
            datetime.now() - timedelta(days=random.randint(0, 1000))
        ).strftime("%d/%m/%Y %H:%M:%S"),
        "INTERACTION_STATUS": random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        "INTERACTION_TYPE": random.choice(["CALL", "CHAT"]),
        "TYPE_OF_CUSTOMER": random.choice(customer_types),
        "INTERACTION_DURATION": int(interaction_duration),
        "TOTAL_TIME": int(
            interaction_duration + random.choice([random.randint(10, 600)])
        ),
        "STATUS_OF_CUSTOMER_INCIDENT": random.choice(
            [
                "RESOLVED",
                "PENDING RESOLUTION",
                "PENDING CUSTOMER UPDATE",
                "WORK IN PROGRESS",
                "TRANSFERRED TO ANOTHER QUEUE",
            ]
        ),
        "RESOLVED_IN_FIRST_CONTACT": random.choice(["YES", "NO"]),
        "SOLUTION_TYPE": random.choice(
            ["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]
        ),
        "RATING": random.choice([random.randint(1, 10)]),
    }


def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_interaction_id: int,
    num_records: int,
) -> None:
    """
    Generate a specified number of new records and optionally update existing records.

    Parameters:
    support_categories (List[str]): List of allowed support categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.
    start_interaction_id (int): The starting INTERACTION_ID for the new records.
    num_records (int): The number of new records to generate.
    """

    interaction_id = start_interaction_id

    for i in range(int(num_records)):
        interaction_id = interaction_id + 1
        new_record = generate_random_record(
            interaction_id, support_categories, agent_pseudo_names, customer_types
        )

        # Introduce NULL values to some fields (up to 10% of data)
        if random.random() < 0.1:
            key_to_nullify = random.choice(
                [
                    "SUPPORT_CATEGORY",
                    "AGENT_PSEUDO_NAME",
                    "CONTACT_DATE",
                    "INTERACTION_STATUS",
                    "INTERACTION_TYPE",
                    "TYPE_OF_CUSTOMER",
                    "INTERACTION_DURATION",
                    "TOTAL_TIME",
                    "STATUS_OF_CUSTOMER_INCIDENT",
                    "SOLUTION_TYPE",
                    "RATING",
                ]
            )
            new_record[key_to_nullify] = None

        NO_SQL_DB_Setup.insert_record(interaction_id, new_record)

        # Introduce updtes to existinf records (up to 25% of data)
        if random.random() < 0.25 and interaction_id > 1:
            update_interaction_id = random.randint(1, interaction_id)
            update_record = generate_random_record(
                update_interaction_id,
                support_categories,
                agent_pseudo_names,
                customer_types,
            )
            NO_SQL_DB_Setup.insert_record(update_interaction_id, update_record)


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

    # Connect to the database
    engine, sessionmaker = connect_to_database(
        config.get("PATH", "SQL_DB_PATH"), config.get("DATABASE", "SQL_DB_NAME")
    )

    # Fetch allowed values from the database
    support_categories: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_SUPPORT_AREAS", "'AMAZON'", "SUPPORT_AREA_NAME"
    )
    agent_pseudo_names: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_AGENTS", "'AMAZON'", "PSEUDO_CODE"
    )
    customer_types: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_CUSTOMER_TYPES", "'AMAZON'", "CUSTOMER_TYPE_NAME"
    )

    # Close the database connection
    close_database_connection(engine)

    # Fetch the maximum RECORD_ID from the existinf JSON file
    max_interaction_id = NO_SQL_DB_Setup.get_max_record_id()

    if max_interaction_id is None:
        max_interaction_id = 0
    else:
        pass

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
    generate_and_update_records(
        support_categories,
        agent_pseudo_names,
        customer_types,
        max_interaction_id,
        num_records,
    )


if __name__ == "__main__":
    main()

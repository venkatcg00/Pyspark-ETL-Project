import os
import random
import configparser
import requests
import argparse
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from typing import List, Dict
from allowed_values import (
    connect_to_database,
    fetch_allowed_values,
    close_database_connection,
)


def get_highest_record_id(api_link) -> int:
    """
    Fetch the highest SUPPORT_IDENTIFIER from the existing XML file.

    Parameters:
    api_link (str): The API link to fetch the highest record ID.

    Returns:
    int: The highest SUPPORT_IDENTIFIER found, or 0 if the file is empty or does not exist.
    """
    try:
        response = requests.get(api_link)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        data = response.json()
        highest_record_id = data.get("highest_record_id")
        if highest_record_id is not None:
            return int(highest_record_id)
        else:
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Error fetching highest record ID: {e}")
        return 0


def upload_record_to_API(api_link, xml_data):
    """
    Send the XML data to the API.

    Parameters:
    api_link (str): The API link to post the record.
    xml_data (ET.Element): The XML data to be sent.
    """
    try:
        # Serialize the XML data to a string
        xml_string = ET.tostring(xml_data, encoding="unicode")

        # Send the XML data to the API
        response = requests.post(
            api_link, data=xml_string, headers={"Content-Type": "application/xml"}
        )
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
    except requests.exceptions.RequestException as e:
        print(f"Error uploading record to API: {e}")


def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> ET.Element:
    """
    Generate a random record with specified data fields.

    Parameters:
    record_id (int): The unique SUPPORT_IDENTIFIER.
    support_categories (List[str]): List of allowed support categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.

    Returns:
    ET.Element: An XML Element representing the generated record.
    """

    contact_duration_seconds = random.randint(20, 600)
    after_contact_work_time_seconds = random.randint(10, contact_duration_seconds - 10)

    record = ET.Element("RECORD")
    ET.SubElement(record, "SUPPORT_IDENTIFIER").text = str(record_id)
    ET.SubElement(record, "CONTACT_REGARDING").text = random.choice(support_categories)
    ET.SubElement(record, "AGENT_CODE").text = random.choice(agent_pseudo_names)
    ET.SubElement(record, "DATE_OF_INTERACTION").text = (
        datetime.now() - timedelta(days=random.randint(0, 1000))
    ).strftime("%Y%m%d%H%M%S")
    ET.SubElement(record, "STATUS_OF_INTERACTION").text = random.choice(
        ["INTERACTION COMPLETED", "CUSTOMER DROPPED", "TRANSFERRED"]
    )
    ET.SubElement(record, "TYPE_OF_INTERACTION").text = random.choice(["CALL", "CHAT"])
    ET.SubElement(record, "CUSTOMER_TYPE").text = random.choice(customer_types)
    ET.SubElement(record, "CONTACT_DURATION").text = str(
        timedelta(seconds=contact_duration_seconds)
    )
    ET.SubElement(record, "AFTER_CONTACT_WORK_TIME").text = str(
        timedelta(seconds=after_contact_work_time_seconds)
    )
    ET.SubElement(record, "INCIDENT_STATUS").text = random.choice(
        [
            "RESOLVED",
            "PENDING RESOLUTION",
            "PENDING CUSTOMER UPDATE",
            "WORK IN PROGRESS",
            "TRANSFERRED TO ANOTHER QUEUE",
        ]
    )
    ET.SubElement(record, "FIRST_CONTACT_SOLVE").text = random.choice(["TRUE", "FALSE"])
    ET.SubElement(record, "TYPE_OF_RESOLUTION").text = random.choice(
        ["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]
    )
    ET.SubElement(record, "SUPPORT_RATING").text = str(
        random.choice([random.randint(1, 5)])
    )
    ET.SubElement(record, "TIME_STAMP").text = str(
        datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    )

    return record


def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_record_id: int,
    num_records: int,
    api_link: str,
):
    """
    Generate a specified number of new records and optionally update existing records.

    Parameters:
    support_categories (List[str]): List of allowed support categories.
    agent_pseudo_names (List[str]): List of allowed agent pseudo names.
    customer_types (List[str]): List of allowed customer types.
    start_record_id (int): The starting SUPPORT_IDENTIFIER for the new records.
    num_records (int): The number of new records to generate.
    api_link (str): The API link to post the records.
    """
    record_id = start_record_id

    for i in range(int(num_records)):
        record_id += 1
        new_record = generate_random_record(
            record_id, support_categories, agent_pseudo_names, customer_types
        )

        # Introduce NULL values to some fields
        if random.random() < 0.1:
            key_to_nullify = random.choice(
                [
                    "CONTACT_REGARDING",
                    "AGENT_CODE",
                    "DATE_OF_INTERACTION",
                    "STATUS_OF_INTERACTION",
                    "TYPE_OF_INTERACTION",
                    "CUSTOMER_TYPE",
                    "CONTACT_DURATION",
                    "AFTER_CONTACT_WORK_TIME",
                    "INCIDENT_STATUS",
                    "FIRST_CONTACT_SOLVE",
                    "SUPPORT_RATING",
                ]
            )
            new_record.find(key_to_nullify).text = None  # type: ignore

        upload_record_to_API(api_link, new_record)

        # Introduce updates to existing records
        if random.random() < 0.25 and record_id > 1:
            update_record_id = random.randint(1, record_id)
            update_record = generate_random_record(
                update_record_id, support_categories, agent_pseudo_names, customer_types
            )
            upload_record_to_API(api_link, update_record)


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

    # Connect to the database
    engine, sessionmaker = connect_to_database(
        config.get("PATH", "SQL_DB_PATH"), config.get("DATABASE", "SQL_DB_NAME")
    )

    # Fetch allowed values from the database
    support_categories: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_SUPPORT_AREAS", "'UBER'", "SUPPORT_AREA_NAME"
    )
    agent_pseudo_names: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_AGENTS", "'UBER'", "PSEUDO_CODE"
    )
    customer_types: List[str] = fetch_allowed_values(
        sessionmaker, "CSD_CUSTOMER_TYPES", "'UBER'", "CUSTOMER_TYPE_NAME"
    )

    # Close the database connection
    close_database_connection(engine)

    host_name = config.get("API", "HOSTNAME")
    port = config.get("API", "PORT")
    api_link_highest_record = f"http://{host_name}:{port}/highest_record_id"
    api_link_add = f"http://{host_name}:{port}/add"

    max_record_id: int = get_highest_record_id(api_link_highest_record)

    if max_record_id is None:
        max_record_id = 0

    # Generate a random number of records if no input is provided
    parser = argparse.ArgumentParser(description="Process some input.")
    parser.add_argument(
        "input", nargs="?", default=None, help="An optional input value"
    )

    args = parser.parse_args()
    num_records: int = args.input

    if num_records is None:
        num_records = random.randint(1, 1000)

    generate_and_update_records(
        support_categories,
        agent_pseudo_names,
        customer_types,
        max_record_id,
        num_records,
        api_link_add,
    )


if __name__ == "__main__":
    main()

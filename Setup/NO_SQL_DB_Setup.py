import os
import configparser
import pickledb
import json


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


# Get the directory where the current Python script is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent directory
project_directory = os.path.dirname(current_directory)

# Construct the path to the parameter file
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

# Read the parameter file
config = configparser.ConfigParser()
config.read(parameter_file_path)

# Extract paths from the parameter file
db_path = config.get("PATH", "NO_SQL_DB_PATH")
db_name = config.get("DATABASE", "NO_SQL_DB_NAME")
db_full_path = os.path.join(db_path, db_name)

# Ensure the directory for the database exists
check_and_create_directory(db_path)

# Check if the database file exists and is not empty
if not os.path.exists(db_full_path) or os.path.getsize(db_full_path) == 0:
    # Create an empty JSON file if it does not exist or is empty
    with open(db_full_path, "w") as db_file:
        db_file.write("{}")


# Function to connect to PickleDB
def connect_to_db():
    return pickledb.load(db_full_path, False)


# Function to initialize the counter if it does not exist
def initialize_counter(db):
    if not db.exists("counter"):
        db.set("counter", 0)
        db.dump()


# Function to get the next auto-incrementing key
def get_next_id(db):
    counter = db.get("counter")
    next_id = counter + 1
    db.set("counter", next_id)
    db.dump()
    return next_id


# Function to insert a record with an auto-incrementing key
def insert_record(key, value):
    db = connect_to_db()
    initialize_counter(db)
    record_id = get_next_id(db)
    db.set(str(record_id), json.dumps({"key": key, "value": value}))
    db.dump()
    return record_id


# Function to query by auto-incrementing key
def query_by_id(record_id, op="eq"):
    db = connect_to_db()
    if op == "eq":
        record = db.get(str(record_id))
        if record:
            return json.loads(record)
        else:
            return None
    elif op == "gt":
        return [
            json.loads(db.get(str(i)))
            for i in range(record_id + 1, db.get("counter") + 1)
            if db.get(str(i))
        ]
    elif op == "lt":
        return [
            json.loads(db.get(str(i))) for i in range(1, record_id) if db.get(str(i))
        ]
    elif op == "ge":
        return [
            json.loads(db.get(str(i)))
            for i in range(record_id, db.get("counter") + 1)
            if db.get(str(i))
        ]
    elif op == "le":
        return [
            json.loads(db.get(str(i)))
            for i in range(1, record_id + 1)
            if db.get(str(i))
        ]
    else:
        raise ValueError("Unsupported operation")


# Function to query by custom key
def query_by_key(key, op="eq"):
    db = connect_to_db()
    results = []
    for i in range(1, db.get("counter") + 1):
        record = db.get(str(i))
        if record:
            record = json.loads(record)
            if op == "eq" and record["key"] == key:
                results.append(record)
            elif op == "gt" and record["key"] > key:
                results.append(record)
            elif op == "lt" and record["key"] < key:
                results.append(record)
            elif op == "ge" and record["key"] >= key:
                results.append(record)
            elif op == "le" and record["key"] <= key:
                results.append(record)
    return results


# Function to get the maximum key value
def get_max_record_id():
    db = connect_to_db()
    max_key = None
    for i in range(1, db.get("counter") + 1):
        record = db.get(str(i))
        if record:
            record = json.loads(record)
            if max_key is None or record["key"] > max_key:
                max_key = record["key"]
    return max_key


# Function to get the maximum auto-increment ID
def get_max_increment_id():
    db = connect_to_db()
    max_id = db.get("counter")
    return max_id

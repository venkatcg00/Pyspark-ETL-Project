import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List
from pathlib import Path
import json
import os
import configparser
import uvicorn
import xml.etree.ElementTree as ET
from datetime import datetime

# Create the app
app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Storage configuration
def check_and_create_directory(directory_path):
    """
    Check if a directory exists and create it if it does not.
    :param directory_path: Path to the directory
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
        except OSError as e:
            logger.error(f"An error occurred while creating the directory: {e}")


# Get the directory where the current Python script is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent directory
project_directory = os.path.dirname(current_directory)

# Construct the path to the parameter file
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

# Read the parameter file
config = configparser.ConfigParser()
config.read(parameter_file_path)

DATA_FOLDER = config.get("PATH", "XML_STORAGE")
ID_FILE = Path(DATA_FOLDER) / "incremental_id.json"
HIGHEST_RECORD_ID_FILE = Path(DATA_FOLDER) / "highest_record_id.json"
MAX_RECORDS = 5000
check_and_create_directory(DATA_FOLDER)

# Ensure the ID file exists
if not Path(ID_FILE).exists():
    with open(ID_FILE, "w") as f:
        json.dump({"incremental_id": 0}, f)

# Ensure the highest record ID file exists
if not Path(HIGHEST_RECORD_ID_FILE).exists():
    with open(HIGHEST_RECORD_ID_FILE, "w") as f:
        json.dump({"highest_record_id": "0"}, f)


# Helper function to load the current incremental ID
def load_incremental_id():
    with open(ID_FILE, "r") as f:
        return json.load(f)["incremental_id"]


# Helper function to save the current incremental ID
def save_incremental_id(value):
    with open(ID_FILE, "w") as f:
        json.dump({"incremental_id": value}, f)


# Helper function to load the highest record ID
def load_highest_record_id():
    with open(HIGHEST_RECORD_ID_FILE, "r") as f:
        return json.load(f)["highest_record_id"]


# Helper function to save the highest record ID
def save_highest_record_id(value):
    with open(HIGHEST_RECORD_ID_FILE, "w") as f:
        json.dump({"highest_record_id": value}, f)


# Data model
class Record(BaseModel):
    id: str  # Provided by the sender
    data: str  # XML data as a string


# Helper function to load all records
def load_records():
    files = sorted(Path(DATA_FOLDER).glob("*.json"), key=os.path.getmtime)
    records = []
    for file in files:
        with open(file, "r") as f:
            records.append(json.load(f))
    return records


# Helper function to save a record
def save_record(increment_id: int, record: Record):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"XML_RECORD_{increment_id}_{timestamp}.json"
    record_file = Path(DATA_FOLDER) / file_name
    with open(record_file, "w") as f:
        json.dump({"increment_id": increment_id, **record.dict()}, f)


# Helper function to maintain record limit
def maintain_limit():
    files = sorted(Path(DATA_FOLDER).glob("*.json"), key=os.path.getmtime)
    while len(files) > MAX_RECORDS:
        os.remove(files[0])
        files.pop(0)


@app.get("/")
async def root():
    return {"message": "Streaming API is running!"}


@app.post("/add")
async def add_record(request: Request):
    """
    Add a new record to the storage. Generates a unique incremental ID.
    """
    # Parse XML data from the request body
    try:
        body = await request.body()
        root = ET.fromstring(body)
        record_id = root.find("SUPPORT_IDENTIFIER").text
        record_data = ET.tostring(root, encoding="unicode")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid XML data: {e}")

    # Create a Record object
    record = Record(id=record_id, data=record_data)

    # Update incremental ID
    incremental_id = load_incremental_id() + 1
    save_incremental_id(incremental_id)

    # Update highest_record_id
    highest_record_id = load_highest_record_id()
    if highest_record_id is None or int(record.id) > int(highest_record_id):
        highest_record_id = record.id
        save_highest_record_id(highest_record_id)
        logger.info(f"Updated highest_record_id to {highest_record_id}")

    # Save the record
    save_record(incremental_id, record)
    maintain_limit()
    return {"message": "Record added successfully.", "increment_id": incremental_id}


@app.get("/get", response_model=List[dict])
async def get_records():
    """
    Retrieve the latest records from the storage.
    """
    records = load_records()
    return records


@app.get("/get/{increment_id}", response_model=dict)
async def get_record(increment_id: int):
    """
    Retrieve a specific record by API-generated incremental ID.
    """
    record_file = Path(DATA_FOLDER) / f"XML_RECORD_{increment_id}_*.json"
    matching_files = list(record_file.parent.glob(record_file.name))
    if not matching_files:
        raise HTTPException(status_code=404, detail="Record not found.")

    with open(matching_files[0], "r") as f:
        record = json.load(f)
    return record


@app.delete("/delete/{increment_id}")
async def delete_record(increment_id: int):
    """
    Delete a specific record by API-generated incremental ID.
    """
    record_file = Path(DATA_FOLDER) / f"XML_RECORD_{increment_id}_*.json"
    matching_files = list(record_file.parent.glob(record_file.name))
    if not matching_files:
        raise HTTPException(status_code=404, detail="Record not found.")

    for file in matching_files:
        os.remove(file)
    return {"message": "Record deleted successfully."}


@app.get("/highest_increment_id")
async def get_highest_increment_id():
    """
    Return the current highest incremental ID.
    """
    incremental_id = load_incremental_id()
    return {"highest_increment_id": incremental_id}


@app.get("/highest_record_id")
async def get_highest_record_id():
    """
    Return the highest record_id received so far.
    """
    highest_record_id = load_highest_record_id()
    if highest_record_id is None:
        return {"highest_record_id": "0"}
    return {"highest_record_id": highest_record_id}


@app.get("/get_greater_than/{increment_id}", response_model=List[dict])
async def get_records_greater_than(increment_id: int):
    """
    Retrieve records with an incremental ID greater than the specified value.
    """
    try:
        files = sorted(Path(DATA_FOLDER).glob("*.json"), key=os.path.getmtime)
        records = []
        for file in files:
            try:
                file_increment_id = int(file.stem.split("_")[2])
            except (IndexError, ValueError) as e:
                logger.error(
                    f"Error parsing increment_id from file name {file.name}: {e}"
                )
                continue
            if file_increment_id > increment_id:
                with open(file, "r") as f:
                    record = json.load(f)
                    records.append(record)
        return records
    except Exception as e:
        logger.error(f"Error fetching records greater than {increment_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


if __name__ == "__main__":
    uvicorn.run("Streaming_API_Setup:app", host="127.0.0.1", port=8000, reload=True)

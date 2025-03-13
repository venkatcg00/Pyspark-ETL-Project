from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import time
from sqlalchemy import create_engine, Table, MetaData, select, func
from sqlalchemy.orm import sessionmaker
import configparser
import os

# Get the directory where the current Python script is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent directory
project_directory = os.path.dirname(current_directory)

# Construct the path to the parameter file
parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

# Read the parameter file
config = configparser.ConfigParser()
config.read(parameter_file_path)

# Extract the parameter values
api_hostname = config.get("API", "HOSTNAME")
api_port = config.get("API", "PORT")
api_url = f"http://{api_hostname}:{api_port}/get"
db_path = config.get("PATH", "SQL_DB_PATH")
db_name = config.get("DATABASE", "SQL_DB_NAME")
db_url = f"sqlite:///{os.path.join(db_path, db_name)}"

# Initialize Spark Session
spark = SparkSession.builder.appName("XML_Straming").getOrCreate()

# Define the schema of the records
schema = StructType(
    [
        StructField("increment_id", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("data", StringType(), True),
    ]
)

# SQLAlchemy database connection
engine = create_engine(db_url)
metadata = MetaData()

# Reflect the existing table
metadata.reflect(bind=engine)
streaming_data_archive = metadata.tables["STREAMING_DATA_ARCHIVE"]

# Create a session
Session = sessionmaker(bind=engine)
session = Session()


# Function to fetch the records from Streaming API
def fetch_records():
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch records: {response.status_code}")


# Function to process new records
def process_new_records(new_records):
    if new_records:
        # Insert new records into the SQLite3 table using SQLAlchemy
        for record in new_records:
            insert_stmt = streaming_data_archive.insert().values(
                STREAM_RECORD_ID=int(record["id"]), STREAMING_DATA=record["data"]
            )
            session.execute(insert_stmt)
        session.commit()


# Function to get the last processed increment ID from the database
def get_last_increment_id():
    max_id_query = select(func.max(streaming_data_archive.c.ARCHIVE_ID))
    result = session.execute(max_id_query).scalar()
    return result if result is not None else 0


# Main function to run the streaming job
def main():
    last_increment_id = get_last_increment_id()

    while True:
        # Fetch records from the FastAPI
        records = fetch_records()

        # Filter new records based on the last_increment_id
        new_records = [
            record
            for record in records
            if record.get("increment_id", 0) > last_increment_id
        ]

        # Process new records
        process_new_records(new_records)

        # Update the last_increment_id
        if new_records:
            last_increment_id = max(record["increment_id"] for record in new_records)

        # Sleep for a while before polling again
        time.sleep(10)


if __name__ == "__main__":
    main()

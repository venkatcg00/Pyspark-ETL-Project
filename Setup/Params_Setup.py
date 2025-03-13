import configparser
import os
import sys

# Get the absolute path of the current script
current_script_path = os.path.abspath(sys.argv[0])


# Make the Repository Path
repo_name = "ETL-Project"
repo_path = current_script_path[: current_script_path.index(repo_name)] + "ETL-Project/"


# Create a ConfigParser object
config = configparser.ConfigParser()


# Add a section and parameters to the configuration
config.add_section("PATH")
config.set("PATH", "CSV_FOLDER", repo_path + "Data/CSV_Files/")
config.set("PATH", "SQL_DML_SCRIPT", repo_path + "Setup/SQL_DML_Script.sql")
config.set("PATH", "SQL_DDL_SCRIPT", repo_path + "Setup/SQL_DDL_Script.sql")
config.set("PATH", "SQL_DB_PATH", repo_path + "Data/Untracked/")
config.set("PATH", "NO_SQL_DB_PATH", repo_path + "Data/Untracked/")
config.set("PATH", "XML_STORAGE", repo_path + "Data/Untracked/XML_STORAGE")


# Add a new section for database details
config.add_section("DATABASE")
config.set("DATABASE", "SQL_DB_NAME", "SQL_DATABASE.db")
config.set("DATABASE", "NO_SQL_DB_NAME", "NO_SQL_DATABASE.db")

# Add a new section for API details
config.add_section("API")
config.set("API", "HOSTNAME", "127.0.0.1")
config.set("API", "PORT", "8000")


# Path to the new configuration file
config_file_path = repo_path + "Setup/Parameters.ini"

# Write the configuration to a file
with open(config_file_path, "w") as configfile:
    config.write(configfile)

import argparse
import boto3
import configparser
from datetime import datetime
import pickle
import os
import logging
from typing import List, Dict, Tuple, Optional, Callable, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFile:
    def __init__(self, filename: str, s3_path: str):
        self.filename: str = filename
        self.s3_path: str = s3_path
        self.date: Optional[datetime.date] = None
        self.time: Optional[str] = None
        self.index: Optional[str] = None  # To store the index if present
        if self.filename.endswith('.csv.gz'):
            self.date, self.time, self.index = self.parse_filename(filename)
    
    def parse_filename(self, filename: str) -> Tuple[Optional[datetime.date], Optional[str], Optional[str]]:
        """
        Parses the filename to extract the date, time, and optional index.
        """
        base_name = filename.split('.')[0]  # Remove file extension
        try:
            # Attempt to parse filename with or without index
            parts = base_name.split('_')
            if len(parts) == 2:
                # Format: date_time.csv.gz
                date_str, time_str = parts
                index = None
            elif len(parts) == 3:
                # Format: date_time_index.csv.gz
                date_str, time_str, index = parts
            else:
                raise ValueError("Unexpected filename format")
    
            date = datetime.strptime(date_str, '%Y%m%d').date()
            return date, time_str, index
        except ValueError as e:
            # Log the filename and the error message
            logger.error(f"Error parsing filename '{filename}': {e}")
            return None, None, None
    
    def __repr__(self) -> str:
        return f"DataFile(filename={self.filename}, date={self.date}, time={self.time}, index={self.index})"
    
class Measurement:
    def __init__(self, name: str):
        self.name: str = name
        self.data_files: List[DataFile] = []
        self.file_counts: Dict[Tuple[datetime.date, str], int] = {}  # To track files per date-time combination
        self.schema: Optional[Dict[str, str]] = None  # To store the schema information if available
        
    def add_data_file(self, data_file: DataFile) -> None:
        self.data_files.append(data_file)
        if data_file.date and data_file.time:
            key = (data_file.date, data_file.time)
            self.file_counts[key] = self.file_counts.get(key, 0) + 1
        
    def set_schema(self, schema_file: str, s3_path: str) -> None:
        self.schema = {
            'schema_file': schema_file,
            's3_path': s3_path
        }
    
    def get_date_range(self) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
        """
        Returns the earliest and latest dates from the data files.
        """
        dates = [data_file.date for data_file in self.data_files if data_file.date]
        if dates:
            return min(dates), max(dates)
        return None, None
    
    def __repr__(self) -> str:
        return f"Measurement(name={self.name}, data_files={self.data_files}, schema={self.schema})"
    
class User:
    def __init__(self, user_id: str):
        self.user_id: str = user_id
        self.measurements: Dict[str, Measurement] = {}
        
    def add_measurement(self, measurement: Measurement) -> None:
        self.measurements[measurement.name] = measurement
        
    def __repr__(self) -> str:
        return f"User(user_id={self.user_id}, measurements={self.measurements})"
    
class S3Bucket:
    SUMMARY_FILENAME = "summary_data.pkl"
    
    def __init__(self, s3_bucket_path: str):
        self.s3_bucket_path: str = s3_bucket_path
        self.users: Dict[str, User] = {}
        self.s3_client = boto3.client('s3')
        self.schemas: Dict[str, str] = {}  # Add this line
    
    def gather_info(self, use_cached: bool = True) -> None:
        """
        Gathers information from the S3 bucket about users, measurements, and data files.
        Checks if a summary file exists and loads data from it if use_cached is True.
        If the file does not exist, fetches data from AWS and creates the file.
        
        :param use_cached: Whether to use cached summary data if available.
        """
        
        if use_cached and os.path.exists(self.SUMMARY_FILENAME):
            self.load_summary_from_file(self.SUMMARY_FILENAME)
        else:
            # Fetch data from AWS and save to the summary file
            logger.info(f"Summary file '{self.SUMMARY_FILENAME}' not found or cache not used. Fetching data from AWS...")
            try:
                bucket_name, prefix = self.s3_bucket_path.split('/', 1)
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
                
                for page in pages:
                    for obj in page.get('Contents', []):
                        key = obj['Key']
                        parts = key[len(prefix):].strip('/').split('/')
                        
                        if len(parts) == 3:
                            user_id, measurement_name, filename = parts
                            
                            user = self.users.setdefault(user_id, User(user_id))
                            measurement = user.measurements.setdefault(measurement_name, Measurement(measurement_name))
                                
                            if filename.endswith('.csv.gz'):
                                # It's a data file
                                data_file = DataFile(filename, key)
                                measurement.add_data_file(data_file)
                            elif filename.endswith('.json'):
                                # It's a schema file
                                if measurement_name not in self.schemas:
                                    schema_content = self.download_schema(bucket_name, key)
                                    self.schemas[measurement_name] = schema_content
                                    measurement.set_schema(filename, key)
                                else:
                                    measurement.set_schema(filename, key)
            except Exception as e:
                logger.error(f"An error occurred while fetching data from AWS: {e}")
                raise
    
            # Save the fetched data to the summary file for future use
            self.save_summary_to_file(self.SUMMARY_FILENAME)

    def download_schema(self, bucket_name: str, key: str) -> str:
        """
        Downloads the schema file content from S3.
        
        :param bucket_name: The name of the S3 bucket.
        :param key: The key (path) to the schema file in S3.
        :return: The content of the schema file as a string.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            return content
        except Exception as e:
            logger.error(f"Failed to download schema from {key}: {e}")
            return ""
                
    def list_all_measurements(self) -> List[str]:
        """
        Lists all unique measurement types across all users.
        
        This method aggregates all measurement types from every user and
        returns them in a unique, sorted list.
        """
        all_measurements = set()
    
        # Iterate over all users and their measurements
        for user in self.users.values():
            all_measurements.update(user.measurements.keys())
        
        # Print all unique measurements
        print("Listing all unique measurements across all users:")
        for measurement_name in sorted(all_measurements):
            print(f"  - {measurement_name}")
        
        return sorted(all_measurements)
    
    def list_all_users(self) -> List[str]:
        """
        Lists all users in the S3 bucket.
        """
        print("Listing all users...")
        for user_id in self.users.keys():
            print(user_id)
        return list(self.users.keys())
    
    def get_measurements_for_user(self, user_id: str) -> List[str]:
        """
        Lists all measurements for a specific user.

        :param user_id: The user ID to list measurements for.
        :return: A list of measurement names.
        """
        if user_id in self.users:
            measurements = list(self.users[user_id].measurements.keys())
            print(f"Measurements for user '{user_id}':")
            for measurement_name in measurements:
                print(f"  - {measurement_name}")
            return measurements
        else:
            print(f"User '{user_id}' not found.")
            return []
    
    def get_users_for_measurement(self, measurement_name: str) -> Dict[str, List[str]]:
        """
        Lists all users who have a specific measurement type.

        :param measurement_name: The measurement name to find users for.
        :return: A dictionary mapping user IDs to their measurements.
        """
        users_with_measurement: Dict[str, List[str]] = {}
        print(f"Listing all users with measurement '{measurement_name}':")
        for user_id, user in self.users.items():
            if measurement_name in user.measurements:
                users_with_measurement[user_id] = list(user.measurements.keys())
                print(f"  - User: {user_id}")
        return users_with_measurement
    
    def generate_summary_report(self) -> None:
        """
        Generates a summary report of the data.
        """
        print("Generating summary report...")
        for user_id, user in self.users.items():
            print(f"User: {user_id}")
            for measurement_name, measurement in user.measurements.items():
                print(f"  Measurement: {measurement_name}")
                start_date, end_date = measurement.get_date_range()
                if start_date and end_date:
                    print(f"    Date range: {start_date} to {end_date}")
                else:
                    print("    Date range: No valid data files")
                
                num_files = len(measurement.data_files)
                print(f"    Number of Files: {num_files}")
    
                if measurement.schema:
                    print(f"    Schema file: {measurement.schema['schema_file']}")
                        
    def update_summary_file(self) -> None:
        """
        Updates the summary file by fetching fresh data from AWS and saving it.
        """
        logger.info("Updating summary file by fetching fresh data from AWS...")
        self.gather_info(use_cached=False)
    
    def check_summary_file(self) -> None:
        """
        Checks if the summary file is present and prints a message indicating its status.
        """
        if os.path.exists(self.SUMMARY_FILENAME):
            print(f"Summary file '{self.SUMMARY_FILENAME}' is present.")
        else:
            print(f"Summary file '{self.SUMMARY_FILENAME}' is not present.")
            
    def save_summary_to_file(self, filename: Optional[str] = None) -> None:
        """
        Saves the summary data to a file using pickle serialization.
        
        :param filename: The name of the file to save the summary data to.
        """
        if filename is None:
            filename = self.SUMMARY_FILENAME
        try:
            with open(filename, 'wb') as file:
                pickle.dump(self.users, file)
                pickle.dump(self.schemas, file)
                logger.info(f"Summary data saved to {filename}.")
        except Exception as e:
            logger.error(f"An error occurred while saving summary data to {filename}: {e}")
    
    def load_summary_from_file(self, filename: Optional[str] = None) -> None:
        """
        Loads the summary data from a file using pickle serialization.
        
        :param filename: The name of the file to load the summary data from.
        """
        if filename is None:
            filename = self.SUMMARY_FILENAME
        try:
            with open(filename, 'rb') as file:
                self.users = pickle.load(file)
                self.schemas = pickle.load(file)
                logger.info(f"Summary data loaded from {filename}.")
        except FileNotFoundError:
            logger.error(f"File {filename} not found. Unable to load summary data.")
        except Exception as e:
            logger.error(f"An error occurred while loading summary data from {filename}: {e}")

    def view_schema(self, measurement_name: str) -> None:
        """
        Displays the contents of the .json schema for the given measurement.

        :param measurement_name: The name of the measurement.
        """
        schema_content = self.schemas.get(measurement_name)

        if schema_content:
            print(f"Schema for measurement '{measurement_name}':\n")
            print(schema_content)
        else:
            print(f"No schema found for measurement '{measurement_name}'.")
    
    def list_commands(self) -> None:
        """
        Lists all available commands.
        """
        commands = [
            "list_all_users",
            "generate_summary_report",
            "update_summary_file",
            "check_summary_file",
            "get_measurements_for_user",
            "list_all_measurements",
            "get_users_for_measurement",
            "view_schema",
            "list_commands"
        ]
        print("Available commands:")
        for cmd in sorted(commands):
            print(f"  - {cmd}")
            
def main():
    # Load configuration from config.ini
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    s3_bucket_path = config['AWS']['s3_bucket_path']
    
    # Create S3Bucket object and gather information
    s3_bucket = S3Bucket(s3_bucket_path)
    s3_bucket.gather_info()
    
    # Set up argument parser for command-line interface
    parser = argparse.ArgumentParser(description="S3 Data Summary Script")

    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    subparsers.required = True  # To make the command required

    # list_all_users command
    subparsers.add_parser('list_all_users', help='List all users')

    # generate_summary_report command
    subparsers.add_parser('generate_summary_report', help='Generate summary report')

    # update_summary_file command
    subparsers.add_parser('update_summary_file', help='Update the summary file by fetching fresh data from AWS')

    # check_summary_file command
    subparsers.add_parser('check_summary_file', help='Check if the summary file is present')

    # get_measurements_for_user command
    parser_get_measurements = subparsers.add_parser('get_measurements_for_user', help='Get measurements for a specific user')
    parser_get_measurements.add_argument('--user_id', type=str, required=True, help='User ID')

    # list_all_measurements command
    subparsers.add_parser('list_all_measurements', help='List all measurements across all users')

    # get_users_for_measurement command
    parser_get_users = subparsers.add_parser('get_users_for_measurement', help='Get users for a specific measurement')
    parser_get_users.add_argument('--measurement_name', type=str, required=True, help='Measurement Name')

    # view_schema command
    parser_view_schema = subparsers.add_parser('view_schema', help='View the schema for a given measurement')
    parser_view_schema.add_argument('--measurement_name', type=str, required=True, help='Measurement Name')


    # list_commands command
    subparsers.add_parser('list_commands', help='List all available commands')

    args = parser.parse_args()

    # Execute the command
    if args.command == 'list_all_users':
        s3_bucket.list_all_users()
    elif args.command == 'generate_summary_report':
        s3_bucket.generate_summary_report()
    elif args.command == 'update_summary_file':
        s3_bucket.update_summary_file()
    elif args.command == 'check_summary_file':
        s3_bucket.check_summary_file()
    elif args.command == 'get_measurements_for_user':
        s3_bucket.get_measurements_for_user(user_id=args.user_id)
    elif args.command == 'list_all_measurements':
        s3_bucket.list_all_measurements()
    elif args.command == 'get_users_for_measurement':
        s3_bucket.get_users_for_measurement(measurement_name=args.measurement_name)
    elif args.command == 'view_schema':
        s3_bucket.view_schema(measurement_name=args.measurement_name)
    elif args.command == 'list_commands':
        s3_bucket.list_commands()
    else:
        parser.print_help()
    
if __name__ == "__main__":
    main()

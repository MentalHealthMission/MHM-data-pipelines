import argparse
import boto3
import configparser
from datetime import datetime
import pickle
import os

class DataFile:
    def __init__(self, filename, s3_path):
        self.filename = filename
        self.s3_path = s3_path
        self.date = None
        self.time = None
        self.index = None  # To store the index if present
        if self.filename.endswith('.csv.gz'):
            self.date, self.time, self.index = self.parse_filename(filename)

    def parse_filename(self, filename):
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
            # Print the filename and the error message
            print(f"Error parsing filename '{filename}': {e}")
            return None, None, None

    def __repr__(self):
        return f"DataFile(filename={self.filename}, date={self.date}, time={self.time}, index={self.index})"

class Measurement:
    def __init__(self, name):
        self.name = name
        self.data_files = []
        self.file_counts = {}  # To track files per date-time combination
        self.schema = None  # To store the schema information if available
    
    def add_data_file(self, data_file):
        self.data_files.append(data_file)
        if data_file.date and data_file.time:
            key = (data_file.date, data_file.time)
            if key not in self.file_counts:
                self.file_counts[key] = 0
            self.file_counts[key] += 1
    
    def set_schema(self, schema_file, s3_path):
        self.schema = {
            'schema_file': schema_file,
            's3_path': s3_path
        }

    def get_date_range(self):
        """
        Returns the earliest and latest dates from the data files.
        """
        dates = [data_file.date for data_file in self.data_files if data_file.date]
        if dates:
            return min(dates), max(dates)
        return None, None

    def __repr__(self):
        return f"Measurement(name={self.name}, data_files={self.data_files}, schema={self.schema})"

class User:
    def __init__(self, user_id):
        self.user_id = user_id
        self.measurements = {}
    
    def add_measurement(self, measurement):
        self.measurements[measurement.name] = measurement
    
    def __repr__(self):
        return f"User(user_id={self.user_id}, measurements={self.measurements})"

class S3Bucket:
    SUMMARY_FILENAME = "summary_data.pkl"

    def __init__(self, s3_bucket_path):
        self.s3_bucket_path = s3_bucket_path
        self.users = {}
        self.s3_client = boto3.client('s3')
        self.commands = {  # Registering commands dynamically
            "list_all_users": self.list_all_users,
            "generate_summary_report": self.generate_summary_report,
            "update_summary_file": self.update_summary_file,
            "check_summary_file": self.check_summary_file,
            "get_measurements_for_user": self.get_measurements_for_user,
            "list_all_measurements": self.list_all_measurements,
            "get_users_for_measurement": self.get_users_for_measurement,
            "list_commands": self.list_commands,  # Registering the list_commands method
        }

    def gather_info(self, use_cached=True):
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
            print(f"Summary file '{self.SUMMARY_FILENAME}' not found or cache not used. Fetching data from AWS...")
            bucket_name, prefix = self.s3_bucket_path.split('/', 1)
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    parts = key[len(prefix):].strip('/').split('/')
                    
                    if len(parts) == 3:
                        user_id, measurement_name, filename = parts
                        
                        if user_id not in self.users:
                            self.users[user_id] = User(user_id)
                            
                        user = self.users[user_id]
                            
                        if measurement_name not in user.measurements:
                            user.add_measurement(Measurement(measurement_name))
                                
                        measurement = user.measurements[measurement_name]
                                
                        if filename.endswith('.csv.gz'):
                            # It's a data file
                            data_file = DataFile(filename, key)
                            measurement.add_data_file(data_file)
                        elif filename.endswith('.json'):
                            # It's a schema file
                            measurement.set_schema(filename, key)

            # Save the fetched data to the summary file for future use
            self.save_summary_to_file(self.SUMMARY_FILENAME)
            
    def list_all_measurements(self):
        """
        Lists all unique measurement types across all users.
        
        This method aggregates all measurement types from every user and
        prints them in a unique, sorted list.
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

    def list_all_users(self):
        """
        Lists all users in the S3 bucket.
        """
        print("Listing all users...")
        for user_id in self.users.keys():
            print(user_id)
        return list(self.users.keys())

    def get_measurements_for_user(self, *args, **kwargs):
        """
        Lists all measurements for a specific user if a user_id is provided,
        or lists all measurements across all users if no user_id is provided.
        
        :param args: Positional arguments (not used).
        :param kwargs: Keyword arguments. Expects 'user_id' as a keyword argument.
        """
        user_id = kwargs.get('user_id', None)
        if user_id:
            if user_id in self.users:
                print(f"Measurements for user '{user_id}':")
                for measurement_name in self.users[user_id].measurements.keys():
                    print(f"  - {measurement_name}")
                return list(self.users[user_id].measurements.keys())
            else:
                print(f"User '{user_id}' not found.")
                return []
        else:
            all_measurements = set()
            print("Listing all measurements across all users:")
            for user in self.users.values():
                all_measurements.update(user.measurements.keys())
        
            for measurement_name in all_measurements:
                print(f"  - {measurement_name}")
            return list(all_measurements)

    def get_users_for_measurement(self, *args, **kwargs):
        """
        Lists all users who have a specific measurement type.
        If no measurement_name is provided, lists all users and their measurements.
        
        :param args: Positional arguments (not used).
        :param kwargs: Keyword arguments. Expects 'measurement_name' as a keyword argument.
        """
        measurement_name = kwargs.get('measurement_name', None)
        users_with_measurement = {}

        if measurement_name:
            print(f"Listing all users with measurement '{measurement_name}':")
            for user_id, user in self.users.items():
                if measurement_name in user.measurements:
                    users_with_measurement[user_id] = user.measurements.keys()
                    print(f"  - User: {user_id}")
        else:
            print("Listing all users and their measurements:")
            for user_id, user in self.users.items():
                measurements = list(user.measurements.keys())
                users_with_measurement[user_id] = measurements
                print(f"  - User: {user_id}, Measurements: {measurements}")

        return users_with_measurement

    def generate_summary_report(self):
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
                    
    def update_summary_file(self, filename="summary_data.pkl"):
        """
        Updates the summary file by fetching fresh data from AWS and saving it.
        
        :param filename: The name of the file to save the summary data to.
        """
        print("Updating summary file by fetching fresh data from AWS...")
        self.gather_info(use_cached=False, filename=filename)

    def check_summary_file(self, filename="summary_data.pkl"):
        """
        Checks if the summary file is present and prints a message indicating its status.
        
        :param filename: The name of the file to check for.
        """
        if os.path.exists(filename):
            print(f"Summary file '{filename}' is present.")
        else:
            print(f"Summary file '{filename}' is not present.")
        
    def save_summary_to_file(self, filename=None):
        """
        Saves the summary data to a file using pickle serialization.
        
        :param filename: The name of the file to save the summary data to.
        """
        if filename is None:
            filename = self.SUMMARY_FILENAME
        with open(filename, 'wb') as file:
            pickle.dump(self.users, file)
            print(f"Summary data saved to {filename}.")

    

    def load_summary_from_file(self, filename=None):
        """
        Loads the summary data from a file using pickle serialization.
        
        :param filename: The name of the file to load the summary data from.
        """
        if filename is None:
            filename = self.SUMMARY_FILENAME
        try:
            with open(filename, 'rb') as file:
                self.users = pickle.load(file)
                print(f"Summary data loaded from {filename}.")
        except FileNotFoundError:
            print(f"File {filename} not found. Unable to load summary data.")
        

    def execute_command(self, command, *args, **kwargs):
        """
        Executes the given command by mapping it to the corresponding method
        in the S3Bucket class.
        
        :param command: A string representing the command to execute.
        :param args: Additional positional arguments for the command.
        :param kwargs: Additional keyword arguments for the command.
        """
        if command in self.commands:
            self.commands[command](*args, **kwargs)  # Pass both args and kwargs
        else:
            print(f"Command '{command}' not recognized. Available commands:")
            self.list_commands()

    def list_commands(self):
        """
        Lists all available commands dynamically by reading the commands dictionary.
        """
        print("Available commands:")
        for cmd in sorted(self.commands.keys()):
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
    parser.add_argument(
        "command",
        type=str,
        help="The command to execute (use list-commands for a list of commands).",
        nargs="?",  # Makes the command optional
        default="generate_summary_report",  # Default command
    )
    parser.add_argument(
        "--user_id",
        type=str,
        help="The user ID to list measurements for (optional).",
        required=False
    )
    parser.add_argument(
        "--measurement_name",
        type=str,
        help="The measurement name to list users for (optional).",
        required=False
    )

    args = parser.parse_args()

    # Prepare kwargs for dynamic argument passing
    kwargs = {}
    if args.user_id:
        kwargs['user_id'] = args.user_id
    if args.measurement_name:
        kwargs['measurement_name'] = args.measurement_name

    # Execute the command with the parsed arguments
    s3_bucket.execute_command(args.command, **kwargs)# Execute the command with the optional user_id


if __name__ == "__main__":
    main()

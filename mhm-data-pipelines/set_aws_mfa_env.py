import boto3
import configparser
import os

def copy_default_to_long_term():
    """
    Copy only the long-term credentials (aws_access_key_id and aws_secret_access_key) from the 'default'
    profile to the 'long-term' profile if the 'long-term' profile does not exist.
    """
    aws_credentials_file = os.path.expanduser('~/.aws/credentials')
    config_parser = configparser.ConfigParser()
    config_parser.read(aws_credentials_file)

    if 'long-term' not in config_parser:
        # Copy only the long-term credentials from 'default' to 'long-term'
        if 'default' in config_parser:
            long_term_profile = {
                'aws_access_key_id': config_parser['default']['aws_access_key_id'],
                'aws_secret_access_key': config_parser['default']['aws_secret_access_key']
            }
            config_parser['long-term'] = long_term_profile

            with open(aws_credentials_file, 'w') as configfile:
                config_parser.write(configfile)

            print("Copied long-term credentials from 'default' to 'long-term' profile.")
        else:
            print("Default profile does not exist. Please set up your AWS CLI with 'aws configure'.")
            exit(1)
    else:
        print("Long-term profile already exists.")

def get_new_session_token(mfa_token_code, mfa_serial):
    """
    Get a new temporary session token using MFA and update the default profile.
    """
    # Create an STS client using the long-term credentials
    session = boto3.Session(profile_name='long-term')
    sts_client = session.client('sts')

    # Request a new session token
    response = sts_client.get_session_token(
        SerialNumber=mfa_serial,
        TokenCode=mfa_token_code
    )

    credentials = response['Credentials']

    # Update ~/.aws/credentials file with the new temporary credentials in the default profile
    aws_credentials_file = os.path.expanduser('~/.aws/credentials')
    config_parser = configparser.ConfigParser()
    config_parser.read(aws_credentials_file)

    # Always overwrite the default profile with new temporary credentials
    if 'default' not in config_parser:
        config_parser.add_section('default')

    config_parser['default']['aws_access_key_id'] = credentials['AccessKeyId']
    config_parser['default']['aws_secret_access_key'] = credentials['SecretAccessKey']
    config_parser['default']['aws_session_token'] = credentials['SessionToken']

    with open(aws_credentials_file, 'w') as configfile:
        config_parser.write(configfile)

    print("Temporary credentials have been updated for the 'default' profile.")
if __name__ == "__main__":
    # Step 1: Copy default credentials to long-term profile if not already copied, excluding session token
    copy_default_to_long_term()

    # Step 2: Read the MFA ARN from config.ini
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    mfa_serial = config['AWS']['mfa_arn']

    # Step 3: Prompt the user for the MFA token code
    mfa_token_code = input("Enter MFA token code: ")

    # Step 4: Get a new session token and update the default profile
    get_new_session_token(mfa_token_code, mfa_serial)

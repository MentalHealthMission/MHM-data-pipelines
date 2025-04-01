import boto3
import configparser
import os

def get_new_session_token(mfa_token_code, profile_name='default'):
    # Read the MFA ARN from config.ini
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    mfa_serial = config['AWS']['mfa_arn']

    # Create an STS client
    session = boto3.Session(profile_name=profile_name)
    sts_client = session.client('sts')

    # Request a new session token
    response = sts_client.get_session_token(
        SerialNumber=mfa_serial,
        TokenCode=mfa_token_code
    )

    credentials = response['Credentials']

    # Update ~/.aws/credentials file with the new temporary credentials
    aws_credentials_file = os.path.expanduser('~/.aws/credentials')
    config_parser = configparser.ConfigParser()
    config_parser.read(aws_credentials_file)

    if profile_name not in config_parser:
        config_parser.add_section(profile_name)

    config_parser[profile_name]['aws_access_key_id'] = credentials['AccessKeyId']
    config_parser[profile_name]['aws_secret_access_key'] = credentials['SecretAccessKey']
    config_parser[profile_name]['aws_session_token'] = credentials['SessionToken']

    with open(aws_credentials_file, 'w') as configfile:
        config_parser.write(configfile)

    print(f"Temporary credentials have been updated for the '{profile_name}' profile.")

if __name__ == "__main__":
    # Prompt the user for the MFA token code
    mfa_token_code = input("Enter MFA token code: ")

    # Get a new session token and update the credentials file
    get_new_session_token(mfa_token_code)

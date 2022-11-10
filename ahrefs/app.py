import os
import json
import boto3

REGION_NAME = os.getenv('AWS_REGION', 'us-east-2')
SECRET_NAME = os.getenv('SECRETS_NAMES', 'lambdas/environments')
ENVIRONMENT = os.getenv('LAMBDA_AHREFS_ENVIRONMENT', 'production')

if ENVIRONMENT == 'dev':
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE', 'expertise'))
else:
    session = boto3.Session()


def lambda_handler(event, context):
    # lambda code goes here
    response = {}

    return {
        'statusCode': 200,
        'body': json.dumps(response, indent=4, default=str)
    }


if __name__ == '__main__':
    lambda_handler({'hours': 4}, None)

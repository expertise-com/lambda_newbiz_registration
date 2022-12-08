import os
import json
import boto3
import psycopg2
import requests

SECRET_NAME = os.getenv('SECRETS_NAMES', 'lambdas/environments')
REGION_NAME = os.getenv('AWS_REGION', 'us-east-2')
ENVIRONMENT = os.getenv('LAMBDA_NEWBIZ_REVIEWME_ENVIRONMENT', 'production')

if ENVIRONMENT == 'dev':
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE', 'expertise'))
    ses_client = session.client('ses')
else:
    session = boto3.Session()
    ses_client = boto3.client('ses')

secrets_client = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME
)

secrets = secrets_client.get_secret_value(
    SecretId=SECRET_NAME
)

if secrets := secrets.get('SecretString'):
    secrets = json.loads(secrets)
    os.environ.update(secrets)

DB_HOST = os.getenv('READ_PRODUCTION_DATABASE_HOST')
DB_DATABASE = os.getenv('READ_PRODUCTION_DATABASE_NAME')
DB_USER = os.getenv('READ_PRODUCTION_DATABASE_USER')
DB_PASSWORD = os.getenv('READ_PRODUCTION_DATABASE_PASSWORD')

db_conn = psycopg2.connect(host=DB_HOST, dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD)


def push_to_salesforce(**data):
    try:
        res = requests.post('https://webto.salesforce.com/servlet/servlet.WebToLead?encoding=UTF-8', params=data)
        res.raise_for_status()
    except Exception as e:
        print(e)  # TODO replace with sentry


def log_to_data_warehouse(**data):
    try:
        db = db_conn.cursor()

        oid = data['oid']
        record_type = data['recordType']
        lead_source = data['lead_source']
        debug = data['debug']
        debug_email = data['debugEmail']
        ret_url = data['retURL']
        reason_for_reaching_out = data['00N3i00000CxJLc']
        email = data['email']
        first_name = data['first_name']
        last_name = data['last_name']
        phone = data['00N3i00000DZFN5']
        company = data['company']
        business_website = data['00N3i00000DEQ9d']
        zipcode = data['Zip_Code__c']
        requested_vertical = data['Requested_Vertical_2__c']

        query = """INSERT INTO sources_reviewmelog (oid, record_type, lead_source, debug, debug_email, ret_url,
                        reason_for_reaching_out, email, first_name, last_name, phone, company, business_website, zipcode,
                        requested_vertical, created, modified)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, NOW(), NOW());"""

        db.execute(query, (
            oid, record_type, lead_source, debug, debug_email, ret_url, reason_for_reaching_out, email, first_name,
            last_name, phone, company, business_website, zipcode, requested_vertical))
        db_conn.commit()
    except Exception as e:
        print(e)  # TODO replace with sentry


def lambda_handler(event, context):

    if event['requestContext']['http']['method'] != 'POST':
        print(event['requestContext']['http']['method'])
        if event['requestContext']['http']['method'] == 'OPTIONS':
            return {"statusCode": 200}
        return {"statusCode": 403}

    try:
        if 'body' in event:
            data = json.loads(event['body'])
        else:
            data = event

        log_to_data_warehouse(**data)
        push_to_salesforce(**data)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message ": 'success'
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error ": repr(e)
            })
        }


if __name__ == '__main__':
    lambda_handler({'requestContext': {'http': {'method': 'POST'}},
  'oid': '00D3i000000pZm6',
  'recordType': '0123i0000005pAlAAI',
  'lead_source': 'New Biz Form',
  'debug': '1',
  'debugEmail': 'chris@expertise.com',
  'retURL': 'https://www.expertise.com/review-me-verification',
  '00N3i00000CxJLc': ["Reach new customers", "Become award-winning", "Stand out from competition"],
  'email': 'email@mail.com',
  'first_name': 'John',
  'last_name': 'Doe',
  '00N3i00000DZFN5': '(310) 123-4567',
  'company': 'Test Company',
  '00N3i00000DEQ9d': 'www.test.com',
  'Zip_Code__c': '90210',
  'Requested_Vertical_2__c': 'a0V6e00000z493KEAQ'
}, None)

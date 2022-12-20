import os
import json
import boto3
import psycopg2
import requests
from simple_salesforce import Salesforce
import sentry_sdk
sentry_sdk.init(
    dsn="https://0704fb74d567486986a5c4c436744801@o851316.ingest.sentry.io/4504362662232064",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)


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
SALESFORCE_USERNAME = os.getenv('SALESFORCE_USERNAME')
SALESFORCE_PASSWORD = os.getenv('SALESFORCE_PASSWORD')
SALESFORCE_SECURITY_TOKEN = os.getenv('SALESFORCE_SECURITY_TOKEN')

SALESFORCE_LEAD_SOURCE = 'Twilio proxy'
SALESFORCE_LEAD_COMPANY = 'Consumer'
SALESFORCE_LEAD_RECORD_TYPE_ID = '0123i0000010ARNAA2'

sf = Salesforce(
    username=SALESFORCE_USERNAME,
    password=SALESFORCE_PASSWORD,
    security_token=SALESFORCE_SECURITY_TOKEN
)

db_conn = psycopg2.connect(host=DB_HOST, dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD)


def get_closest_directory_salesforce_id(zipcode, vertical):
    try:
        query = f"""SELECT directory_salesforce_id FROM (
                      SELECT z.zip_code,
                             --calculate distance is a routine (function) stored in the DW -- guessing there are more efficient ways to calculate this in backend?
                             calculate_distance(z.lat::NUMERIC, z.lng::NUMERIC, m.lat::NUMERIC, m.lng::NUMERIC,
                                                'M') AS distance,
                             d.id AS directory_id,
                             d.salesforce_id AS directory_salesforce_id,
                             v.id AS vertical_id,
                             v.name AS vertical_name,
                             m.name AS metro_name,
                             d.is_live,
                             d.directory_link,
                             rank() over (partition BY z.zip_code ORDER BY calculate_distance(z.lat::NUMERIC, z.lng::NUMERIC, m.lat::NUMERIC, m.lng::NUMERIC,
                                                'M') ASC) AS metro_rank
                      FROM reporting_directory d
                               LEFT JOIN manual_zipcodes z ON 1=1
                               LEFT JOIN reporting_metro m ON d.metro_id = m.id
                               LEFT JOIN reporting_vertical v ON d.vertical_id = v.id
                     --INPUTS
                        WHERE z.zip_code IN ('{zipcode}') AND v.salesforce_id = '{vertical}'
                     --REQUIREMENTS
                  ) main
            --REQUIREMENTS
            WHERE is_live = TRUE ORDER BY distance limit 1;"""
        db = db_conn.cursor()
        db.execute(query)
        rows = db.fetchall()
        return rows[0][0]
    except Exception as e:
        sentry_sdk.capture_exception(e)


def push_to_salesforce(**data):
    try:
        res = requests.post('https://webto.salesforce.com/servlet/servlet.WebToLead?encoding=UTF-8', params=data)
        res.raise_for_status()
        zipcode = data['Zip_Code__c']
        requested_vertical = data['Requested_Vertical_2__c']

        closest_directory_salesforce_id = get_closest_directory_salesforce_id(zipcode, requested_vertical)

        tpv = sf.query(f"SELECT Top_Provider_Value__c FROM Directory__c where Id = '{closest_directory_salesforce_id}' ")['records'][0]['Top_Provider_Value__c']

        describe = sf.Inbound_Lead_Prioritization__mdt.describe()
        fields = [field['name'] for field in describe['fields']]
        fields_str = ''
        for field in fields:
            fields_str += field + ", "

        cutoffs = sf.query(
            "SELECT " + fields_str[:-2] + " FROM Inbound_Lead_Prioritization__mdt ORDER BY Min_Value_Cutoff__c DESC")[
            'records']
        for cutoff in cutoffs:
            if tpv >= cutoff['Min_Value_Cutoff__c']:
                return json.dumps(cutoff)

    except Exception as e:
        sentry_sdk.capture_exception(e)


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
        sentry_sdk.capture_exception(e)


def lambda_handler(event, context):

    if event['httpMethod'] != 'POST':
        print(event['httpMethod'])
        if event['httpMethod'] == 'OPTIONS':
            return {"statusCode": 200}
        return {"statusCode": 403}

    try:
        if 'body' in event:
            data = json.loads(event['body'])
        else:
            data = event

        log_to_data_warehouse(**data)
        return_details = push_to_salesforce(**data)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": 'success',
                "details": return_details
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
    print(lambda_handler({'requestContext': {'http': {'method': 'POST'}},
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
  'Zip_Code__c': '90093',
  'Requested_Vertical_2__c': 'a0V3i000000yVJDEA2'
}, None))

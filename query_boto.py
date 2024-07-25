# https://frankcorso.dev/querying-athena-python.html
# https://medium.com/@vtbs55596/how-to-query-common-crawl-data-using-amazon-athena-416ad13e54f8
# https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format
# https://commoncrawl.org/get-started

#%%
import os
import boto3

#%%
AWS_KEY = os.getenv('AWS_KEY')
AWS_SECRET = os.getenv('AWS_SECRET')
AWS_REGION = "us-east-1"
S3_STAGING_DIR = 's3://omgbananarepublic/'
SCHEMA_NAME = 'cc-index'

#%%
########################################
# boto3
########################################
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

#%%
########################################
# [query] submit query
########################################
query = '''
SELECT url,
       url_host_name,
    -- content_languages,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2024-26' -- filter by crawl
  AND subset = 'warc' -- filter by subset
  AND url_host_tld = 'com' -- domain must end with .com
  AND fetch_status = 200 -- must be successful request
  AND content_mime_type = 'text/html' -- only care about htmls
  AND content_languages LIKE '%zho%' -- must contain chinese
  AND url_host_name LIKE '%hk%' -- url_host_name must contain hk
  AND url_host_registered_domain = 'yahoo.com' -- focus on yahoo.com domain
  AND warc_record_length > 150000 -- focus on large records
ORDER BY warc_record_length ASC
'''
query_execution = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        #"EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)
execution_id = query_execution['QueryExecutionId']

# %%
########################################
# [query] check query status
########################################
query_details = athena_client.get_query_execution(
        QueryExecutionId=execution_id
    )
state = query_details['QueryExecution']['Status']['State']
'''
SUCCEEDED
FAILED
CANCELLED
'''

# %%
########################################
# [query] get query results
########################################
response_query_result = athena_client.get_query_results(
        QueryExecutionId=execution_id
    )
print(response_query_result['ResultSet']['Rows'])


'''
# LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';

query_execution = athena_client.start_query_execution(
    QueryString=query,
    QueryExecutionContext={
        'Database': 'ccindex',
    },
)
execution_id = query_execution['QueryExecutionId']
'''
# %%

# %%
########################################
# [query] wait until success
########################################

import time

while True:
    time.sleep(1)
    query_details = athena_client.get_query_execution(
        QueryExecutionId=execution_id
    )
    state = query_details['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        response_query_result = athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        # Do something with results
        break


# %%
########################################
# [query] check n times
########################################

import time

for i in range(15):
    time.sleep(1)
    query_details = athena_client.get_query_execution(
        QueryExecutionId=execution_id
    )
    state = query_details['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        response_query_result = athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        # Do something with results

# Do something if we never got a success

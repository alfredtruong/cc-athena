import os
import pandas as pd
from pathlib import Path
import re
import hashlib
from sqlalchemy.engine import create_engine
from sqlalchemy import text
from urllib.parse import quote_plus

########################################
# setup aws
########################################
# can also do `aws configure` as per https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
from dotenv import load_dotenv
load_dotenv(".env") # save AWS_KEY and AWS_SECRET in .env file

import os
AWS_KEY = quote_plus(os.getenv('AWS_KEY'))
AWS_SECRET = quote_plus(os.getenv('AWS_SECRET'))
AWS_REGION = 'us-east-1'
SCHEMA_NAME = 'ccindex'
S3_STAGING_DIR = quote_plus('s3://omgbananarepublic/')

########################################
# class to query / cache tables in athena
########################################
class AthenaQuery:
  '''
  cached AWS Athena queries
  '''
  def __init__(self, path: str = 'data/queries/', testing: bool = False) -> None:
    self.conn_str = 'awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}'

    self.engine = create_engine(
      self.conn_str.format(
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
        schema_name=SCHEMA_NAME,
        s3_staging_dir=S3_STAGING_DIR
      )
    )

    self.query_output_path = Path(path)

    self.testing = testing

  def touch_filepath(self, _hash:str) -> Path:
    return self.query_output_path / f'{_hash}'

  def results_filepath(self, _hash:str) -> Path:
    return self.query_output_path / f'{_hash}_results.pkl'

  def query_filepath(self, _hash:str) -> Path:
    return self.query_output_path / f'{_hash}_query.txt'

  def strip_query(self, query:str) -> str:
    query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)  # remove comments
    query = re.sub(r'\s+', ' ', query) # Remove whitespace
    return query.strip()
  '''
  stripped_query = strip_query(query)
  '''

  def hash_query(self, query:str) -> str:
    stripped_query = self.strip_query(query) # standardize
    #hasher = hashlib.sha256() # longer hashes
    hasher = hashlib.md5() # shorter hashes
    hasher.update(stripped_query.encode('utf-8'))
    return hasher.hexdigest()
  '''
  hash_query(query)
  '''

  def query_athena(self, query:str) -> pd.DataFrame:
    print('query AWS')
    return pd.read_sql(text(query),self.engine)

  def do_query(self, query:str) -> pd.DataFrame:
    _hash = self.hash_query(query)
    results_fp = self.results_filepath(_hash)
    if results_fp.exists():
      print(f'read cached results for {_hash}')
      return pd.read_pickle(results_fp)
    else:
      # ensure output directory exists
      query_fp = self.query_filepath(_hash)
      if not query_fp.parent.exists():
        query_fp.mkdir(parents=True, exist_ok=True)

      # is testing or not
      if self.testing:
        print('would have done a query')
        return
      
      # do query
      df = self.query_athena(query)

      # save query
      df.to_pickle(results_fp)

      # save hash_query.txt
      with open(query_fp,'w') as f:
        f.write(query)

      # save touch file
      Path(self.touch_filepath(_hash)).touch()

      # logging
      print(_hash)

      # return results
      return df

  def load_cached_result(self, _hash:str) -> pd.DataFrame:
      # show query
      with open(self.query_filepath(_hash),'r',encoding='utf-8') as f:
        for line in f.readlines():
          print(line,end='')

      # return results
      print('read cached results')
      return pd.read_pickle(self.results_filepath(_hash))

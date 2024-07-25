# WHAT
- query common crawl (columnar) index that sits natively on S3
- send SQL queries to AWS Athena directly from Python

# APPROACHES
- use sqlalchemy + pyathena
- use boto3

# FEATURES
- queries and results are automatically cached locally
- cached results are used if sql query is the same

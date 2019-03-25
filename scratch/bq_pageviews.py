# Get page views for each url from BigQuery
from google.cloud import bigquery
import os
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('addons').getOrCreate()

project = 'ga-mozilla-org-prod-001'
client = bigquery.Client(project=project)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/sarahmelancon/addons/credentials.json"

query = (
    "SELECT * FROM `ga-mozilla-org-prod-001.67693596.ga_sessions_20190324` "
    "LIMIT 100"
)

query_job = client.query(
    query,
    location="US",
)

dict_list = [dict(row.items()) for row in query_job]

pageViews = [row['totals']['pageviews'] for row in dict_list]
screenName = [row['hits'][0]['appInfo']['screenName'] for row in dict_list]
d = [{'pageViews': views, 'screenName': name} for views, name in zip(pageViews, screenName)]
df = spark.createDataFrame(d)
pageview_count = df.groupby('screenName').agg(F.sum('pageViews').alias('pageViews'))

print(pageview_count.show(5))

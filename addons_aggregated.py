import pandas as pd
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql.types import *
import datetime
from moztelemetry import Dataset

yesterday_str = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(1), '%Y%m%d')

raw_pings = (
  Dataset.from_source("telemetry")
  .where(docType="main")
  .where(appUpdateChannel="release")
  .where(submissionDate=lambda x: x.startswith(yesterday_str))
  .records(sc, sample=.01)
)

raw_pings_crash = (
  Dataset.from_source("telemetry")
  .where(docType="crash")
  .where(appUpdateChannel="release")
  .where(submissionDate=lambda x: x.startswith(yesterday_str))
  .records(sc, sample=.01)
)

summary_last_year = (
  spark.sql("""
  SELECT *
  FROM main_summary
  WHERE 
    (submission_date_s3 >= (NOW() - INTERVAL 365 DAYS))
    and normalized_channel = 'release'
    and sample_id = '42'
  """)
  .withColumn('submission_date', F.to_timestamp('submission_date_s3', 'yyyyMMdd'))
  .drop('submission_date_s3')
)
# This file contains helper functions to create the BigQuery portion of the dataset
# TODO

# from google.cloud import bigquery
import os
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime

# spark = SparkSession.builder.appName('addons').getOrCreate()


def load_bq(date, json_credentials_path, spark):
    """
    Creates data frame with the number of page views each slug recieved on the given date
    """
    today = date.strftime("%Y%m%d")

    project = "ga-mozilla-org-prod-001"
    client = bigquery.Client(project=project)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_credentials_path

    query = (
        "SELECT * FROM `ga-mozilla-org-prod-001.67693596.ga_sessions_" + today + "`"
        "LIMIT 100"
    )

    query_job = client.query(query, location="US")

    dict_list = [dict(row.items()) for row in query_job]

    pageViews = [row["totals"]["pageviews"] for row in dict_list]
    screenName = [row["hits"][0]["appInfo"]["screenName"] for row in dict_list]
    d = [
        {"pageViews": views, "screenName": name}
        for views, name in zip(pageViews, screenName)
    ]
    df = spark.createDataFrame(d)

    get_slug = F.udf(lambda x: x.split("/")[-2])

    pageview_count = (
        df.filter(
            F.col("screenName").startswith("addons.mozilla.org/af/firefox/addon/")
        )
        .groupby("screenName")
        .agg(F.sum("pageViews").alias("pageViews"))
        .withColumn("slug", get_slug("screenName"))
        .drop("screenName")
    )

    return pageview_count

# This file contains a helper function to create the AMO portion of the dataset
# This is copied over from Databricks - it probably won't work as is

from pyspark.sql import SQLContext

def load_amo():
    db = 'db-slave-amoprod1.amo.us-west-2.prod.mozaws.net:3306'
    hostname, port = db.split(":")
    port = int(port)
    database = 'addons_mozilla_org'

    tempdir = "s3n://mozilla-databricks-telemetry-test/amo-mysql/_temp"
    jdbcurl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}&ssl=true&sslMode=verify-ca" \
              .format(hostname, port, database, 
                      dbutils.secrets.get("amo-mysql","amo-mysql-user"), 
                      dbutils.secrets.get("amo-mysql","amo-mysql-pass"))

    print(jdbcurl)
    sql_context = SQLContext(sc)

    amo_df = sql_context.read \
             .format("jdbc") \
             .option("forward_spark_s3_credentials", True) \
             .option("url", jdbcurl) \
             .option("tempdir", tempdir) \
             .option("query", "select guid, averagerating, totalreviews from addons limit 1000") \
             .load()
from addons_daily.utils.telemetry_data import *
from addons_daily.utils.helpers import *
from addons_daily.addons_report import *
from pyspark.sql.types import *
from pyspark.sql import Row
import json
import os


BASE_DATE = "20190515"

def load_json(filename):
    root = os.path.dirname(__file__)
    path = os.path.join(root, "resources", filename)
    with open(path) as f:
        d = json.load(f)
    return d


def load_df_from_json(prefix, spark):
    root = os.path.dirname(__file__)
    schema_path = os.path.join(root, "resources", "{}_schema.json".format(prefix))
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = os.path.join(root, "resources", "{}.json".format(prefix))
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    frame = spark.read.json(rows_path, schema, mode="FAILFAST")
    return frame


def df_to_json(df):
    return [i.asDict() for i in df.collect()]


spark = SparkSession.builder.appName("addons_daily_tests").getOrCreate()
sc = SparkContext.getOrCreate()


main_summary = load_df_from_json("main_summary", spark)
search_clients_daily = load_df_from_json("search_clients_daily", spark)
events = load_df_from_json("events", spark)
raw_pings = sc.parallelize(load_json("raw_pings.json"))

# addons_expanded = expand_addons(main_summary)

# print(df_to_json(get_is_system(addons_expanded)))

agg = agg_addons_report(
    spark=spark,
    date=BASE_DATE,
    main_summary=main_summary,
    search_clients_daily=search_clients_daily,
    events=events,
    raw_pings=raw_pings,
)

agg.printSchema()
print(df_to_json(agg))

# with open("TEST.json", "w") as f:
#         f.write(str(df_to_json(agg)))




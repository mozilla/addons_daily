from helper_functions import take_top_ten
from pyspark.sql.types import *
import pyspark.sql.functions as F


def top_ten(df):
    """
    Given a dataframe drawn from main_summary,
    return a dataframe of the form:
    addon_id : [addon_id_1, addon_id_2, ... , addon_id10]
    for the top ten other addons used with a given addon
    :param df: dataframe of main_summary rows
    :return: dataframe as described above
    """
    ttt = F.udf(take_top_ten, ArrayType(StringType()))

    client_to_addon = (
        df
        .rdd
        .filter(lambda x: x['active_addons'] is not None)
        .map(lambda x: (x['client_id'], [y['addon_id'] for y in x['active_addons']]))
        .filter(lambda x: len(x[1]) > 1)
        .map(lambda x: (x[0], [(x[1][i], [y for y in x[1] if y != x[1][i]])
                               for i in range(len(x[1])) if len(x[1]) > 1]))
        .filter(lambda x: len(x[1]) > 2)
        .flatMap(lambda x: [(str(x[0]), element[0], element[1]) for element in x[1]])
        .flatMap(lambda x: [(x[1], j, x[0]) for j in x[2]])
    )

    schema = StructType([StructField("addon_id", StringType(), True),
                     StructField("other_addons", StringType(), True),
                     StructField("client_id", StringType(), True)])

    other_addons = client_to_addon.toDF(schema=schema)

    map_df = (
        other_addons
        .groupBy("addon_id", "other_addons")
        .agg(F.countDistinct("client_id"))
    )

    temp_rdd = (
        map_df
        .rdd.map(lambda x: (x[0], (x[1], x[2])))
        .groupByKey()
        .map(lambda x: (x[0], {y[0]: float(y[1]) for y in x[1]}))
    )

    other_addons_schema = StructType([StructField("addon_id", StringType(), True),
                                  StructField("other_addons", MapType(StringType(), FloatType()), True)])

    other_addons_df = (
        temp_rdd
        .toDF(schema=other_addons_schema)
        .withColumn("top_ten_other_addons", ttt("other_addons"))
        .drop("other_addons")
    )
    return other_addons_df
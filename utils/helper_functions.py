import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession


make_map = F.udf(lambda x, y: dict(zip(x, y)), MapType(StringType(), DoubleType()))

def get_spark():
    spark = (SparkSession
             .builder
             .appName("extension_data")
             .getOrCreate())

    return spark

def histogram_mean(values):
    """
    Returns the mean of values in a histogram.
    This mean relies on the sum *post*-quantization, which amounts to a
    left-hand-rule discrete integral of the histogram. It is therefore
    likely to be an underestimate of the true mean.
    """
    if values is None:
        return None
    numerator = 0
    denominator = 0
    for k, v in values.items():
        numerator += int(k) * v
        denominator += v
    if denominator == 0:
        return None
    return numerator / float(denominator)


def get_hist_avg(hist,just_keyed_hist):
    """
    :param hist: name of histogram of interest
    :param just_keyed_hist: pyspark dataframe of
    :return: returns a pyspark dataframe aggregated in the following form:
    addon_id : mean(hist)
    """
    hist_data = (
        just_keyed_hist
        .filter(lambda x: hist in x.keys())
        .map(lambda x: x[hist])
        .flatMap(lambda x: [(i, histogram_mean(x[i]['values'])) for i in x.keys()])
    )

    agg_schema = StructType([StructField("addon_id", StringType(), True),
                             StructField("avg_" + hist.lower(), FloatType(), True)])

    return hist_data.toDF(schema=agg_schema)


def dataframe_joiner(dfs):
    """
    Given a list of dataframes, join them all on "addon_id",
    and return the joined dataframe
    For use in keyed_histograms.py
    :param dfs: list of pyspark aggregated dfs
    :return: one joined df of all the dataframes in dfs
    """
    left = dfs[0]
    for right in dfs[1:]:
        left = left.join(right,on="addon_id",how=left)
    return left


def take_top_ten(dic):
    lis = []
    a1_sorted_keys = sorted(dic, key=dic.get, reverse=True)
    for r in a1_sorted_keys:
        if len(lis) < 10:
            lis.append(r)
    return lis

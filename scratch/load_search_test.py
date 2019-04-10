def get_dest(output_bucket, output_prefix, output_version, date=None, sample_id=None):
    '''
    Stiches together an s3 destination.
    :param output_bucket: s3 output_bucket
    :param output_prefix: s3 output_prefix (within output_bucket)
    :param output_version: dataset output_version
    :retrn str ->
    s3://output_bucket/output_prefix/output_version/submissin_date_s3=[date]/sample_id=[sid]
    '''
    suffix = ''
    if date is not None:
        suffix += "/submission_date_s3={}".format(date)
    if sample_id is not None:
        suffix += "/sample_id={}".format(sample_id)
    full_dest = 's3://' + '/'.join([output_bucket, output_prefix, output_version]) + suffix + '/'
    return full_dest


def get_spark(tz='UTC'):
    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    spark.conf.set('spark.sql.session.timeZone', tz)

    return spark


def load_search_daily(spark, input_bucket, input_prefix, input_version):
    """
    Not sure how to load search_daily from s3
    """
    dest = get_dest(input_bucket, input_prefix, input_version)
    return (spark
            .read
            .option("mergeSchema", True)
            .parquet(dest))


def main():
    path = '' # need to pass in from command line i think
    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    spark.conf.set('spark.sql.session.timeZone', tz)
    sc = get_sc()

    sd = load_search_daily(spark, input_bucket='telemetry-parquet', input_prefix='load_search_daily', input_version='v4')
    search_daily = (
        sd
        .filter("submission_date >= (NOW() - INTERVAL 1 DAYS)")
        .select('client_id')
    )
    print(search_daily.show(5))

if __name__ == '__main__':
    main()

    

import pyspark.sql.functions as F

def install_flow_events(events_df):
    """
    
    """
    install_flow_events = (
        events_df
        .select(["client_id",
                 "submission_date_s3",
                 'event_map_values.method',
                 'event_method',
                 'event_string_value',
                 'event_map_values.source',
                 'event_map_values.download_time',
                 'event_map_values.addon_id',
        ])
        .filter("event_object = 'extension'")
        .filter(
        """
        (event_method = 'install' and event_map_values.step = 'download_completed') or 
        (event_method = 'uninstall')
        """)
        .withColumn("addon_id", F.when(F.col("addon_id").isNull(), 
            F.col("event_string_value"))
            .otherwise(F.col("addon_id"))) # uninstalls populate addon_id in a different place
        .drop("event_string_value")
        .groupby("addon_id", "event_method", "source")
        .agg(
            F.avg("download_time").alias("avg_download_time"),
            F.countDistinct("client_id").alias("n_distinct_users")
        )
    )

    number_installs = (
        install_flow_events
        .where(install_flow_events.event_method == 'install')
        .groupby('addon_id')
        .agg(F.sum('n_distinct_users').alias('installs'))
    )

    number_uninstalls = (
        install_flow_events
        .where(install_flow_events.event_method == 'uninstall')
        .groupby('addon_id')
        .agg(F.sum('n_distinct_users').alias('uninstalls'))
    )

    install_flow_events_df = number_installs.join(number_uninstalls, 'addon_id', how='full')

    return install_flow_events_df
def make_search_daily_data():
    search_daily_sample = [
        Row(
            client_id=u"9bcc2356-f241-4a57-bd51-78bab300312f",
            Submission_date=datetime.datetime(2019, 4, 14, 0, 0),
            sap=3,
            tagged_sap=None,
            organic=6,
        ),
        Row(
            client_id=u"c3ef033b-d436-4dd8-84c8-95bbb1eafcca",
            Submission_date=datetime.datetime(2019, 4, 14, 0, 0),
            sap=1,
            tagged_sap=6,
            organic=None,
        ),
        Row(
            client_id=u"734ccbab-cc80-6d40-b4b4-c1de28ee6f44",
            Submission_date=datetime.datetime(2019, 4, 14, 0, 0),
            sap=1,
            tagged_sap=None,
            organic=1,
        ),
        Row(
            client_id=u"13e24161-85b7-4c1e-98e6-b58b64481bd2",
            Submission_date=datetime.datetime(2019, 4, 14, 0, 0),
            sap=7,
            tagged_sap=10,
            organic=None,
        ),
        Row(
            client_id=u"f5a03324-1c16-4143-88fa-f4306f6d2359",
            Submission_date=datetime.datetime(2019, 4, 14, 0, 0),
            sap=1,
            tagged_sap=None,
            organic=8,
        ),
    ]

    search_daily_schema = StructType(
        [
            StructField("client_id", StringType(), True),
            StructField("Submission_date", TimestampType(), True),
            StructField("sap", IntegerType(), True),
            StructField("tagged_sap", IntegerType(), True),
            StructField("organic", IntegerType(), True),
        ]
    )

    return search_daily_sample, search_daily_schema

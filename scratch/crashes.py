def crashes(raw_pings_crash):
	"""
	"""
	addon_time = (
		raw_pings_crash
		.filter(lambda x: 'environment' in x.keys())
		.filter(lambda x: 'addons' in x['environment'].keys())
		.filter(lambda x: 'activeAddons' in x['environment']['addons'])
		.map(lambda x: (x['environment']['addons']['activeAddons'].keys(), x['creationDate']))
		.map(lambda x: [(i, x[1]) for i in x[0]])
		.flatMap(lambda x: x)
	)

	dateToHour = F.udf(lambda x: x.hour, IntegerType())

	addon_time_df = (
		spark.createDataFrame(addon_time, ['addon_id', 'time'])
		.withColumn('time_stamp', F.to_timestamp('time', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
		.withColumn('hour', dateToHour('time_stamp'))
		.drop('time', 'time_stamp')
	)

	crashes_df = (
		addon_time_df
		.groupby('addon_id', 'hour')
		.agg(F.count(F.lit(1)).alias('crashes'))
		.groupby('addon_id')
		.agg((F.sum('crashes') / F.sum('hour')).alias('avg_hourly_crashes'))
	)

	return crashes_df

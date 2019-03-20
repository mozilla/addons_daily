import pyspark.sql.functions as F


def tab_switch_time(raw_pings):
    """
    Given an rdd of keyed histograms taken from raw pings, calculate the average tab switch 
    time for each addon
    """
    tab_switch_hist = (
    raw_pings
    .filter(lambda x: 'environment' in x.keys())
    .filter(lambda x: 'addons' in x['environment'].keys())
    .filter(lambda x: 'activeAddons' in x['environment']['addons'])
    .filter(lambda x: 'payload' in x.keys())
    .filter(lambda x: 'histograms' in x['payload'].keys())
    .filter(lambda x: 'FX_TAB_SWITCH_TOTAL_E10S_MS' in x['payload']['histograms'].keys())
    .map(lambda x: (x['environment']['addons']['activeAddons'].keys(), x['payload']['histograms']
                    ['FX_TAB_SWITCH_TOTAL_E10S_MS']))
    .map(lambda x: [(i, x[1]) for i in x[0]])
    .flatMap(lambda x: x))
    
    tab_switch = tab_switch_hist.map(lambda x: (x[0], histogram_mean(x[1]['values'])))
    
    tab_switch_df = (
        spark.createDataFrame(tab_switch, ['addon_id', 'tab_switch_ms'])
        .groupby('addon_id')
        .agg(F.mean('tab_switch_ms').alias('tab_switch_ms')))

    return tab_switch_df

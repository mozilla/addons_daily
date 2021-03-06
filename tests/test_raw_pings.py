from pyspark.sql.types import *
from pyspark.sql import Row
from addons_daily.utils.raw_pings import *
from addons_daily.utils.helpers import load_keyed_hist
import json
import datetime
import pytest


def load_expected_data(filename, spark):
    root = os.path.dirname(__file__)
    path = os.path.join(root, "resources", filename)
    with open(path) as f:
        d = json.load(f)
    return d


@pytest.fixture()
def raw_pings(spark):
    sc = spark.sparkContext
    return load_keyed_hist(sc.parallelize(load_expected_data("raw_pings.json", spark)))


def test_startup_time(raw_pings):
    output = get_startup_time(raw_pings).collect()
    expected_output = [
        Row(addon_id="screenshots@mozilla.org", avg_startup_time=4087.0),
        Row(addon_id="fxmonitor@mozilla.org", avg_startup_time=4059.75),
        Row(addon_id="mozilla_cc3@internetdownloadmanager.com", avg_startup_time=807.0),
        Row(addon_id="browser-mon@xdman.sourceforge.net", avg_startup_time=888.0),
        Row(addon_id="@hoxx-vpn", avg_startup_time=733.0),
        Row(addon_id="{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}", avg_startup_time=454.0),
        Row(addon_id="formautofill@mozilla.org", avg_startup_time=4076.5),
        Row(addon_id="firefox@ghostery.com", avg_startup_time=14354.0),
        Row(addon_id="jid1-NIfFY2CA8fy1tg@jetpack", avg_startup_time=977.0),
        Row(addon_id="webcompat@mozilla.org", avg_startup_time=4059.75),
        Row(addon_id="plg@frhadiadsk", avg_startup_time=807.0),
        Row(addon_id="uBlock0@raymondhill.net", avg_startup_time=888.0),
    ]

    assert output == expected_output


def test_bkgd_load_time(raw_pings):
    output = get_bkgd_load_time(raw_pings).collect()

    print("OUTPUT", output)
    expected_output = [
        Row(
            addon_id="screenshots@mozilla.org",
            avg_webext_background_page_load_ms_=2172.75,
        ),
        Row(
            addon_id="fxmonitor@mozilla.org", avg_webext_background_page_load_ms_=1969.5
        ),
        Row(
            addon_id="mozilla_cc3@internetdownloadmanager.com",
            avg_webext_background_page_load_ms_=1577.0,
        ),
        Row(
            addon_id="browser-mon@xdman.sourceforge.net",
            avg_webext_background_page_load_ms_=964.0,
        ),
        Row(addon_id="@hoxx-vpn", avg_webext_background_page_load_ms_=964.0),
        Row(
            addon_id="{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}",
            avg_webext_background_page_load_ms_=535.0,
        ),
        Row(
            addon_id="formautofill@mozilla.org",
            avg_webext_background_page_load_ms_=1958.25,
        ),
        Row(
            addon_id="firefox@ghostery.com", avg_webext_background_page_load_ms_=7609.0
        ),
        Row(
            addon_id="jid1-NIfFY2CA8fy1tg@jetpack",
            avg_webext_background_page_load_ms_=1577.0,
        ),
        Row(
            addon_id="webcompat@mozilla.org",
            avg_webext_background_page_load_ms_=1849.25,
        ),
        Row(addon_id="plg@frhadiadsk", avg_webext_background_page_load_ms_=1295.0),
        Row(
            addon_id="uBlock0@raymondhill.net",
            avg_webext_background_page_load_ms_=964.0,
        ),
    ]

    assert output == expected_output


def test_storage_set(raw_pings):
    output = get_storage_local_set_time(raw_pings).collect()
    assert output == [
        Row(
            addon_id="{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}",
            avg_storage_local_set_ms=2.75,
        )
    ]


def test_storage_get(raw_pings):
    output = get_storage_local_get_time(raw_pings).collect()

    assert output == [
        Row(
            addon_id="{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}",
            avg_storage_local_get_ms=2.3333332538604736,
        )
    ]


def test_pa_popup(raw_pings):
    output = get_pa_popup_load_time(raw_pings).collect()
    assert output == [
        Row(addon_id="izer@camelcamelcamel.com", avg_pa_popup_load_time=75.75)
    ]


def test_cs_injection(raw_pings):
    output = get_cs_injection_time(raw_pings).collect()
    expected = [
        Row(
            addon_id="{b9db16a4-6edc-47ec-a1f4-b86292ed211d}",
            avg_content_script_injection_ms=1.6136363744735718,
        )
    ]
    assert expected == output


def test_ba_popup(raw_pings):
    output = get_ba_popup_load_time(raw_pings).collect()
    assert output == [
        Row(addon_id="firefox@ghostery.com", avg_ba_popup_load_time=340.0)
    ]

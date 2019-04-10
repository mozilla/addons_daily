from pyspark.sql.types import *
from pyspark.sql import Row
import datetime
from utils.raw_pings import *
import pytest


@pytest.fixture()
def raw_pings():
    return [5,6]


def test_g(raw_pings):
    assert raw_pings == [5, 6]




import os

import pytest

# local Spark only; bind to loopback so it works on laptops and CI runners without a resolvable hostname
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("cdc-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.host", "127.0.0.1")
               .getOrCreate())
    yield session
    session.stop()

import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# Try common locations of the project's src/
CANDIDATES = [
    Path(__file__).resolve().parents[1] / "src",
    Path(__file__).resolve().parents[2] / "src",
    Path.cwd() / "src",
]
for c in CANDIDATES:
    if c.exists() and c.is_dir():
        sys.path.insert(0, str(c))

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("quantfoundry_tests")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    yield spark
    spark.stop()

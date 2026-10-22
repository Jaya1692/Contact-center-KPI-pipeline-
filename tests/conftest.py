import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def spark():
    from kpi_pipeline.spark_session import get_spark
    s = get_spark("kpi-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("raw")
    subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_data.py"), str(d)], check=True)
    return str(d)


@pytest.fixture(scope="session")
def pipeline_output(spark, raw_dir, tmp_path_factory):
    from kpi_pipeline.run_pipeline import run
    base = str(tmp_path_factory.mktemp("lake"))
    result = run(raw_dir, base, spark=spark)
    return base, result

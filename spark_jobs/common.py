"""Shared runtime configuration and paths for SparkSkew-Benchmark.

The project is developed on Windows.  It deliberately supports a project-local
portable JDK 17 / winutils pair under ``.bootstrap`` so that running the demo
never requires changing the machine's system PATH or JAVA_HOME.  If those
portable runtimes are absent, the standard Spark environment variables are
used instead.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "behavior.csv"
ODS_PATH = PROJECT_ROOT / "data" / "warehouse" / "ods" / "behavior_raw"
DWD_PATH = PROJECT_ROOT / "data" / "warehouse" / "dwd" / "behavior_events"
DWS_ITEM_STATS_PATH = PROJECT_ROOT / "data" / "warehouse" / "dws" / "item_stats"
FEATURE_ROOT = PROJECT_ROOT / "data" / "warehouse" / "features"
USER_FEATURES_PATH = FEATURE_ROOT / "user_features"
ITEM_FEATURES_PATH = FEATURE_ROOT / "item_features"
FEATURE_RESULTS_PATH = FEATURE_ROOT / "summary.json"
ALS_MODEL_PATH = PROJECT_ROOT / "data" / "warehouse" / "models" / "als_implicit"
BENCHMARK_RESULTS_PATH = PROJECT_ROOT / "benchmark_results.json"
BENCHMARK_SWEEP_RESULTS_PATH = PROJECT_ROOT / "benchmark_salt_sweep.json"
EVALUATION_RESULTS_PATH = PROJECT_ROOT / "evaluation_results.json"


def _prepend_path(directory: Path) -> None:
    value = str(directory)
    existing = os.environ.get("PATH", "")
    parts = existing.split(os.pathsep) if existing else []
    if value not in parts:
        os.environ["PATH"] = value + os.pathsep + existing


def configure_runtime() -> None:
    """Configure the driver/worker runtime before PySpark starts its JVM.

    Priority:
    1. Project-local portable JDK 17 (``.bootstrap/jdk17``) on Windows.
    2. An existing ``JAVA_HOME`` supplied by the operator.

    Set ``SPARKINSIGHT_USE_SYSTEM_JAVA=1`` to keep a system-provided JDK even
    when the portable JDK is present.  Spark 3.5.1 officially supports JDK 8,
    11 and 17, so this project pins JDK 17 and never assumes Java 21 works.
    """

    use_system_java = os.environ.get("SPARKINSIGHT_USE_SYSTEM_JAVA") == "1"
    local_jdk_root = PROJECT_ROOT / ".bootstrap" / "jdk17"
    local_jdks = sorted(local_jdk_root.glob("jdk-17*")) if local_jdk_root.exists() else []
    if local_jdks and not use_system_java:
        java_home = local_jdks[-1]
        os.environ["JAVA_HOME"] = str(java_home)
        _prepend_path(java_home / "bin")

    # Spark's Hadoop client invokes winutils.exe on Windows.  hadoop.dll must
    # sit beside winutils.exe in HADOOP_HOME/bin.  Without these binaries Spark
    # can fail while creating local warehouse directories or querying users.
    local_hadoop = PROJECT_ROOT / ".bootstrap" / "hadoop"
    if (local_hadoop / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(local_hadoop)
        _prepend_path(local_hadoop / "bin")

    # The driver and the Python workers must use the same interpreter.  This is
    # especially important after activating the project's .venv on Windows.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


configure_runtime()

from pyspark.sql import SparkSession  # noqa: E402  (JAVA_HOME must be set first)


def fs_path(path: Path) -> str:
    """Return a Spark-friendly forward-slash path on Windows."""

    return str(Path(path).resolve()).replace("\\", "/")


def build_spark_session(
    app_name: str,
    *,
    shuffle_partitions: int = 16,
    enable_aqe: bool = False,
    master: str | None = None,
) -> SparkSession:
    """Create a deterministic local Spark session.

    AQE is disabled by default.  That is intentional for the skew benchmark:
    adaptive skew-join handling and coalescing would hide the very shuffle
    problem the salted aggregation is meant to expose.  The benchmark pins the
    same values explicitly instead of relying on Spark defaults.
    """

    master = master or os.environ.get("SPARKINSIGHT_MASTER", "local[8]")
    aqe = "true" if enable_aqe else "false"
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", aqe)
        .config("spark.sql.adaptive.coalescePartitions.enabled", aqe)
        .config("spark.sql.adaptive.skewJoin.enabled", aqe)
        .config("spark.sql.adaptive.localShuffleReader.enabled", aqe)
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.warehouse.dir", fs_path(PROJECT_ROOT / "data" / "warehouse"))
        .config("spark.driver.maxResultSize", "256m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dwd_exists() -> None:
    if not DWD_PATH.exists():
        raise FileNotFoundError(
            f"DWD not found at {DWD_PATH}. Run: "
            "python spark_jobs/run_pipeline.py"
        )

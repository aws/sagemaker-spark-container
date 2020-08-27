"""Utility functions for configuring and running the Spark history server."""
import logging
import re
import subprocess
import sys
from typing import Optional

from smspark.bootstrapper import Bootstrapper

SPARK_DEFAULTS_CONFIG_PATH = "conf/spark-defaults.conf"

CONFIG_HISTORY_LOG_DIR_FORMAT = "spark.history.fs.logDirectory={}"
CONFIG_LOCAL_HISTORY_UI_PORT = "spark.history.ui.port=15050"
CONFIG_NOTEBOOK_PROXY_BASE = "spark.ui.proxyBase=/proxy/15050"

# TODO (amoeller@): Every file has the same log config, need common place for
# consistent logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%m-%d %H:%M",
)
log = logging.getLogger("sagemaker-spark")


def start_history_server(event_logs_s3_uri: str) -> None:
    """Bootstrap the history server instance and starts the Spark history server instance."""
    bootstrapper = Bootstrapper()
    log.info("copying aws jars")
    bootstrapper.copy_aws_jars()
    log.info("copying cluster config")
    bootstrapper.copy_cluster_config()
    log.info("copying history server config")
    config_history_server(event_logs_s3_uri)
    log.info("bootstrap master node")
    bootstrapper.start_spark_standalone_primary()

    try:
        subprocess.run("sbin/start-history-server.sh", check=True, shell=True)
    except Exception as e:
        log.error("Error starting history server", e)
        sys.exit(255)


def config_history_server(event_logs_s3_uri: str) -> None:
    """Configure the Spark history server."""
    _config_history_log_dir(event_logs_s3_uri)
    _config_proxy_base()


def _config_history_log_dir(event_logs_s3_uri: Optional[str]) -> None:
    if event_logs_s3_uri is not None:
        log.info("s3 path presents, starting history server")

        # s3 path has to be the format s3://{bucket}/{folder}, Hadoop’s “S3A” client
        # offers high-performance IO against Amazon S3 object store and compatible
        # implementations, but we need to keep this abstraction away from customers,
        # customers only need to know their s3 path and container converts it to
        # s3a://{bucket}/{folder}
        # TODO (guoqiao): EMRFS should support talking to s3 directly according to https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html
        s3_path = re.sub("s3://", "s3a://", event_logs_s3_uri, 1)

        with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
            print(CONFIG_HISTORY_LOG_DIR_FORMAT.format(s3_path))
            spark_config.write(CONFIG_HISTORY_LOG_DIR_FORMAT.format(s3_path) + "\n")
    else:
        log.info("Env variable HISTORY_LOG_DIR does not exist, exiting")
        exit(
            "The SPARK_EVENT_LOGS_S3_URI environment variable was not specified, please specify a valid s3 path for the Spark event logs."
        )


# The method sets spark.ui.proxyBase, otherwise the Spark UI resource cannot be found. The history server only supported in two cases: 1) local machine 2)
# notebook instance. The presence of Env variable "NOTEBOOK_INSTANCE" indicates if we need to update spark.ui.proxyBase or not, this variable is passed from
# python sdk
def _config_proxy_base() -> None:
    with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
        spark_config.write(CONFIG_NOTEBOOK_PROXY_BASE + "\n")

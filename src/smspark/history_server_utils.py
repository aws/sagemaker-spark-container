# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""Utility functions for configuring and running the Spark history server."""
import logging
import subprocess
import sys
import traceback
from typing import Optional

from smspark.bootstrapper import Bootstrapper
from smspark.errors import AlgorithmError, InputError

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
    log.info("setting regional configs")
    bootstrapper.set_regional_configs()
    log.info("copying history server config")
    config_history_server(event_logs_s3_uri)
    log.info("bootstrap master node")
    bootstrapper.start_spark_standalone_primary()

    try:
        subprocess.run("sbin/start-history-server.sh", check=True)
    except subprocess.CalledProcessError as e:
        raise AlgorithmError(message=e.stderr.decode(sys.getfilesystemencoding()), caused_by=e, exit_code=e.returncode)
    except Exception as e:
        log.error("Exception during processing: " + str(e) + "\n" + traceback.format_exc())
        raise AlgorithmError(
            message="error occurred during start-history-server execution. Please see logs for details.", caused_by=e,
        )


def config_history_server(event_logs_s3_uri: str) -> None:
    """Configure the Spark history server."""
    _config_history_log_dir(event_logs_s3_uri)
    _config_proxy_base()


def _config_history_log_dir(event_logs_s3_uri: Optional[str]) -> None:
    if event_logs_s3_uri is not None:
        log.info("s3 path presents, starting history server")

        with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
            print(CONFIG_HISTORY_LOG_DIR_FORMAT.format(event_logs_s3_uri))
            spark_config.write(CONFIG_HISTORY_LOG_DIR_FORMAT.format(event_logs_s3_uri) + "\n")
    else:
        log.info("event_logs_s3_uri does not exist, exiting")
        raise InputError(
            ValueError("spark event logs s3 uri was not specified"), message="Failed to configure history server"
        )


# The method sets spark.ui.proxyBase, otherwise the Spark UI resource cannot be found. The history server only supported in two cases: 1) local machine 2)
# notebook instance. The presence of Env variable "NOTEBOOK_INSTANCE" indicates if we need to update spark.ui.proxyBase or not, this variable is passed from
# python sdk
def _config_proxy_base() -> None:
    with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
        spark_config.write(CONFIG_NOTEBOOK_PROXY_BASE + "\n")

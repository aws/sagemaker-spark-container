"""Manages the lifecycle of a running Spark job."""
import json
import logging
import os
import socket
import subprocess
import sys
import time
import traceback
from os import path
from typing import Any, Dict

from smspark.bootstrapper import Bootstrapper
from smspark.defaults import (default_processing_job_config,
                              default_resource_config)
from smspark.spark_event_logs_publisher import SparkEventLogPublisher
from smspark.spark_executor_logs_watcher import SparkExecutorLogsWatcher
from tenacity import retry, stop_after_delay


class ProcessingJobManager(object):
    """Manages the lifecycle of a Spark job."""

    port = 8080

    def __init__(
        self, resource_config: Dict[str, Any] = None, processing_job_config: Dict[str, Any] = None,  # type: ignore
    ) -> None:
        """Initialize a ProcessingJobManager, loading configs if not provided."""

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("smspark-submit")

        try:
            resource_config_path = "/opt/ml/config/resourceconfig.json"
            with open(resource_config_path, "r") as f:
                self._resource_config = json.load(f)
        except Exception:
            self.logger.warning(
                "Could not read resource config file at {}. Using default resourceconfig.".format(resource_config_path)
            )
            self._resource_config = default_resource_config

        self.logger.info(self._resource_config)

        try:
            processing_job_config_path = "/opt/ml/config/processingjobconfig.json"
            with open(processing_job_config_path, "r") as f:
                self._processing_job_config = json.load(f)
        except Exception:
            self.logger.warning(
                "Could not read resource config file at {}. Using default resourceconfig.".format(resource_config_path)
            )
            self._processing_job_config = default_processing_job_config

        self.logger.info(self._processing_job_config)
        self.bootstrapper = Bootstrapper(self._resource_config)

    @property
    def _is_primary_host(self) -> bool:
        current_host = self._resource_config["current_host"]  # type: ignore
        return current_host == self._cluster_primary_host

    @property
    def _cluster_primary_host(self) -> str:
        return sorted(self._resource_config["hosts"])[0]  # type: ignore

    def _wait_for_hostname_resolution(self) -> None:
        for host in self._resource_config["hosts"]:  # type: ignore
            self._dns_lookup(host)

    @retry(stop=stop_after_delay(60))
    def _dns_lookup(self, host: str) -> None:
        socket.gethostbyname(host)

    def run(self, spark_submit_cmd: str) -> None:
        """Run a Spark job.

        First, wait for workers to come up and bootstraps the cluster.
        Then runs spark-submit, waits until the job succeeds or fails.
        Worker nodes are shut down gracefully.

        Args:
          spark_submit_cmd (str): Command submitted to run spark-submit
        """
        self.logger.info("waiting for hosts")
        self._wait_for_hostname_resolution()
        self.logger.info("bootstrapping cluster")
        self._bootstrap_yarn()
        self.logger.info("starting executor logs watcher")
        self._watch_executor_logs()
        if self._is_primary_host:
            self.logger.info(f"running {spark_submit_cmd}")
            self._run_spark_submit(spark_submit_cmd)
            self.logger.info("spark submit finished. Writing end of job file")
            return_code = subprocess.Popen(["hdfs", "dfs", "-touchz", "/_END_OF_JOB"]).wait()
            if return_code != 0:
                self._write_output_message("Could not create end of job file. Failing job.")
                sys.exit(255)
            time.sleep(15)
            self.logger.info("primary node exiting")
            sys.exit(0)
        else:
            self._wait_for_primary_to_shut_down()

    def _wait_for_primary_to_shut_down(self) -> None:
        while True:
            return_code = subprocess.Popen(["hdfs", "dfs", "-stat", "/_END_OF_JOB"], stderr=subprocess.DEVNULL).wait()
            if return_code == 0:
                self.logger.info("worker node exiting")
                sys.exit(0)
            time.sleep(5)

    def _run_spark_submit(self, spark_submit_cmd: str) -> None:
        spark_log_publisher = SparkEventLogPublisher()
        spark_log_publisher.start()

        # SparkEventLogPublisher writes event log config to spark-defaults.conf, give it 1 seconds.
        time.sleep(1)
        try:
            subprocess.run(spark_submit_cmd, check=True, shell=True)
        except Exception as e:
            self.logger.info("Exception during processing: " + str(e) + "\n" + traceback.format_exc())
            self._write_output_message("Exception during processing: " + str(e))
            sys.exit(255)
        finally:
            spark_log_publisher.down()

    def _write_output_message(self, message: str) -> None:
        if not path.exists("/opt/ml/output"):
            os.makedirs("/opt/ml/output")

        with open("/opt/ml/output/message", "w") as output_message_file:
            output_message_file.write(message)

    def _bootstrap_yarn(self) -> None:
        self.bootstrapper.bootstrap_smspark_submit()
        # TODO: wait for hadoop poll 172.17.0.2:8088/ws/v1/cluster/info

    def _watch_executor_logs(self, log_dir="/var/log/yarn"):
        # TODO: check Yarn configs for yarn.log.dir/YARN_LOG_DIR, in case of overrides
        spark_executor_logs_watcher = SparkExecutorLogsWatcher(log_dir)
        spark_executor_logs_watcher.daemon = True
        spark_executor_logs_watcher.start()

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
"""Manages the lifecycle of a running Spark job."""
import json
import logging
import socket
import subprocess
import traceback
from typing import Any, Dict, Mapping, Sequence

from requests.exceptions import ConnectionError
from smspark.bootstrapper import Bootstrapper
from smspark.defaults import default_processing_job_config, default_resource_config
from smspark.errors import AlgorithmError
from smspark.spark_event_logs_publisher import SparkEventLogPublisher
from smspark.spark_executor_logs_watcher import SparkExecutorLogsWatcher
from smspark.status import Status, StatusApp, StatusClient, StatusMessage, StatusServer
from smspark.waiter import Waiter
from tenacity import retry, stop_after_delay


class ProcessingJobManager(object):
    """Manages the lifecycle of a Spark job."""

    def __init__(
        self,
        resource_config: Dict[str, Any] = None,  # type: ignore
        processing_job_config: Dict[str, Any] = None,  # type: ignore
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
        self.waiter = Waiter()
        self.status_app = StatusApp()
        self.status_client = StatusClient()

    @property
    def hostname(self) -> str:
        """Return the current host's hostname."""
        return self._resource_config["current_host"]

    @property
    def hosts(self) -> Sequence[str]:
        """Return a sequence of all the hostnames in the cluster."""
        return self._resource_config["hosts"]

    @property
    def _is_primary_host(self) -> bool:
        current_host = self.hostname
        return current_host == self._cluster_primary_host

    @property
    def _cluster_primary_host(self) -> str:
        return sorted(self._resource_config["hosts"])[0]

    def _wait_for_hostname_resolution(self) -> None:
        for host in self._resource_config["hosts"]:
            self._dns_lookup(host)

    @retry(stop=stop_after_delay(60))
    def _dns_lookup(self, host: str) -> None:
        socket.gethostbyname(host)

    def run(self, spark_submit_cmd: str, spark_event_logs_s3_uri: str, local_spark_event_logs_dir: str) -> None:
        """Run a Spark job.

        First, wait for workers to come up and bootstraps the cluster.
        Then runs spark-submit, waits until the job succeeds or fails.
        Worker nodes are shut down gracefully.

        Args:
          spark_submit_cmd (str): Command submitted to run spark-submit
        """
        self.logger.info("waiting for hosts")
        self._wait_for_hostname_resolution()
        self.logger.info("starting status server")
        self._start_status_server()
        self.logger.info("bootstrapping cluster")
        self._bootstrap_yarn()
        self.logger.info("starting executor logs watcher")
        self._start_executor_logs_watcher()

        if self._is_primary_host:
            self.logger.info("start log event log publisher")
            spark_log_publisher = self._start_spark_event_log_publisher(
                spark_event_logs_s3_uri, local_spark_event_logs_dir
            )

            self.logger.info(f"Waiting for hosts to bootstrap: {self.hosts}")

            def all_hosts_have_bootstrapped() -> bool:
                try:
                    host_statuses: Mapping[str, StatusMessage] = self.status_client.get_status(self.hosts)

                except ConnectionError as e:
                    self.logger.info(
                        f"Got ConnectionError when polling hosts for status. Host may not have come up: {str(e)}.\nTraceback: {traceback.format_exc()}"
                    )
                    return False
                self.logger.info(f"Received host statuses: {host_statuses.items()}")
                has_bootstrapped = [message.status == Status.WAITING for message in host_statuses.values()]
                return all(has_bootstrapped)

            self.waiter.wait_for(predicate_fn=all_hosts_have_bootstrapped, timeout=180.0, period=5.0)

            try:
                subprocess.run(spark_submit_cmd, check=True, shell=True)
                self.logger.info("spark submit was successful. primary node exiting.")
            except subprocess.CalledProcessError as e:
                self.logger.error(
                    f"spark-submit command failed with exit code {e.returncode}: {str(e)}\n{traceback.format_exc()}"
                    + str(e)
                    + "\n"
                    + traceback.format_exc()
                )
                raise AlgorithmError("spark failed with a non-zero exit code", caused_by=e, exit_code=e.returncode)
            except Exception as e:
                self.logger.error("Exception during processing: " + str(e) + "\n" + traceback.format_exc())
                raise AlgorithmError(
                    message="error occurred during spark-submit execution. Please see logs for details.", caused_by=e,
                )

            finally:
                spark_log_publisher.down()
                spark_log_publisher.join(timeout=20)

        else:
            # workers wait until the primary is up, then wait until it's down.
            def primary_is_up() -> bool:
                try:
                    self.status_client.get_status([self._cluster_primary_host])
                    return True
                except Exception:
                    return False

            def primary_is_down() -> bool:
                return not primary_is_up()

            self.logger.info("waiting for the primary to come up")
            self.waiter.wait_for(primary_is_up, timeout=60.0, period=1.0)
            self.logger.info("waiting for the primary to go down")
            self.waiter.wait_for(primary_is_down, timeout=float("inf"), period=5.0)
            self.logger.info("primary is down, worker now exiting")

    def _bootstrap_yarn(self) -> None:
        self.status_app.status = Status.BOOTSTRAPPING
        self.bootstrapper.bootstrap_smspark_submit()
        self.status_app.status = Status.WAITING

    def _start_executor_logs_watcher(self, log_dir: str = "/var/log/yarn") -> None:
        # TODO: check Yarn configs for yarn.log.dir/YARN_LOG_DIR, in case of overrides
        spark_executor_logs_watcher = SparkExecutorLogsWatcher(log_dir)
        spark_executor_logs_watcher.daemon = True
        spark_executor_logs_watcher.start()

    def _start_status_server(self) -> None:
        server = StatusServer(self.status_app, self.hostname)
        server.daemon = True
        server.start()

    def _start_spark_event_log_publisher(
        self, spark_event_logs_s3_uri: str, local_spark_event_logs_dir: str
    ) -> SparkEventLogPublisher:
        spark_log_publisher = SparkEventLogPublisher(spark_event_logs_s3_uri, local_spark_event_logs_dir)
        spark_log_publisher.daemon = True
        spark_log_publisher.start()
        return spark_log_publisher

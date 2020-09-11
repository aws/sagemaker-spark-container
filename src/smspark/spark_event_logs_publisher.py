"""Thread to offload Spark events to S3 for the history server."""
import logging
import os
import re
import time
from shutil import copyfile
from threading import Thread
from typing import Optional, Sequence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
)
log = logging.getLogger("sagemaker-spark-event-logs-publisher")

SPARK_DEFAULTS_CONFIG_PATH = "conf/spark-defaults.conf"
CONFIG_ENABLE_EVENT_LOG = "spark.eventLog.enabled=true"
CONFIG_EVENT_LOG_DIR_FORMAT = "spark.eventLog.dir={}"
EVENT_LOG_DIR = "/tmp/spark-events/"


class SparkEventLogPublisher(Thread):
    """Child thread copy the spark event logs to a place can be recognized by SageMaker.

    The spark.history.fs.logDirectory will be set to /tmp/spark-events, where the spark events will be
    sent to locally. This SparkEventLogPublisher will copy the event logs file to the dst passed from
    python sdk. The path stores in local_spark_event_logs_dir.
    """

    def __init__(
        self, spark_event_logs_s3_uri: Optional[str], local_spark_event_logs_dir: Optional[str], copy_interval: int = 20
    ) -> None:
        """Initialize."""
        Thread.__init__(self)
        self._stop_publishing = False
        self._copy_interval = copy_interval
        self.spark_event_logs_s3_uri = spark_event_logs_s3_uri
        self.local_spark_event_logs_dir = local_spark_event_logs_dir

    def run(self) -> None:
        """Publish spark events to the given S3 URL.

        If spark_event_logs_s3_uri is specified, spark events will be published to
        s3 via spark's s3a client.
        """
        if self.spark_event_logs_s3_uri is not None:
            log.info("spark_event_logs_s3_uri is specified, publishing to s3 directly")
            self._config_event_log_with_s3_uri()
            return

        if not self.local_spark_event_logs_dir:
            log.info("Spark event log not enabled.")
            return

        log.info("Start to copy the spark event logs file.")
        self._config_event_log()
        dst_dir = self.local_spark_event_logs_dir

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)

        while not self._stop_publishing:
            self._copy_spark_event_logs(EVENT_LOG_DIR, dst_dir)
            time.sleep(self._copy_interval)

        # After stop, should still try to perform the last copy, otherwise the last part may be missed
        self._copy_spark_event_logs(EVENT_LOG_DIR, dst_dir)

    def down(self) -> None:
        """Stop publishing Spark events."""
        self._stop_publishing = True

    def _copy_spark_event_logs(self, src_dir: str, dst_dir: str) -> None:
        src_file_names = self._get_src_file_names(src_dir)

        for src_file_name in src_file_names:
            src = src_dir + src_file_name
            dst = dst_dir + self._get_dst_file_name(src_file_name)
            logging.info("copying {} to {}".format(src, dst))
            copyfile(src, dst)

    # this method returns all
    def _get_src_file_names(self, src_dir: str) -> Sequence[str]:
        files = os.listdir(src_dir)
        if len(files) != 0:
            log.info("Got spark event logs file: " + files[0])
            return files
        else:
            log.info("Event log file does not exist.")
            return []

    # remove .inprogress if present, otherwise original src file name
    def _get_dst_file_name(self, src_file_name: str) -> str:
        return re.sub(".inprogress", "", src_file_name)

    def _config_event_log(self) -> None:
        # By default, spark event log is not enabled, it will be enabled when customers
        # require to publish spark event log to s3. In that case, it will be first written to
        # a local file, and then published to s3.
        if not os.path.exists(EVENT_LOG_DIR):
            os.makedirs(EVENT_LOG_DIR)
        with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
            log.info("Writing event log config to spark-defaults.conf")
            spark_config.write(CONFIG_ENABLE_EVENT_LOG + "\n")
            spark_config.write(CONFIG_EVENT_LOG_DIR_FORMAT.format(EVENT_LOG_DIR) + "\n")

    def _config_event_log_with_s3_uri(self) -> None:
        if self.spark_event_logs_s3_uri:
            s3_path = re.sub("s3://", "s3a://", self.spark_event_logs_s3_uri, 1)
            with open(SPARK_DEFAULTS_CONFIG_PATH, "a") as spark_config:
                spark_config.write(CONFIG_ENABLE_EVENT_LOG + "\n")
                spark_config.write(CONFIG_EVENT_LOG_DIR_FORMAT.format(s3_path) + "\n")

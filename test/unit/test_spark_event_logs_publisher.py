import pytest
import os
import time

from unittest.mock import call
from unittest.mock import patch
from unittest.mock import mock_open
from smspark.spark_event_logs_publisher import SparkEventLogPublisher
from smspark.spark_event_logs_publisher import CONFIG_ENABLE_EVENT_LOG
from smspark.spark_event_logs_publisher import CONFIG_EVENT_LOG_DIR_FORMAT
from smspark.spark_event_logs_publisher import SPARK_DEFAULTS_CONFIG_PATH
from smspark.spark_event_logs_publisher import EVENT_LOG_DIR


SPARK_LOCAL_EVENT_LOG_DIR = "spark/spark-events/"
TEST_FILE_CONTENT = "test"
TEST_WRITE_COUNT = 4
EVENT_FILE_NAME = "file_name"
EVENT_FILE_SUFFIX = ".inprogress"

@pytest.fixture(scope="function")
def event_log_dir_env_var():
    os.environ["SPARK_LOCAL_EVENT_LOG_DIR"] = SPARK_LOCAL_EVENT_LOG_DIR
    yield
    del os.environ["SPARK_LOCAL_EVENT_LOG_DIR"]

@patch("smspark.spark_event_logs_publisher.copyfile")
@patch("os.makedirs")
@patch("os.listdir")
@patch("os.path.exists")
@patch("smspark.spark_event_logs_publisher.open", new_callable=mock_open)
def test_run_with_event_log_dir(mock_open_file,
                                mock_os_path_exists,
                                mock_os_listdir,
                                mock_os_makedirs,
                                mock_copy_file,
                                event_log_dir_env_var):
    mock_os_path_exists.return_value = False
    mock_os_listdir.side_effect = [[], [EVENT_FILE_NAME + EVENT_FILE_SUFFIX]]

    publisher = SparkEventLogPublisher(copy_interval=5)
    publisher.start()

    # For unit test purpose, sleep for 2 sec so while loop only run once
    time.sleep(2)
    publisher.down()
    publisher.join()

    mock_os_makedirs.assert_has_calls([
        call(EVENT_LOG_DIR),
        call(SPARK_LOCAL_EVENT_LOG_DIR)
    ])

    mock_open_file.assert_called_with(SPARK_DEFAULTS_CONFIG_PATH, "a")

    mock_open_file().write.assert_has_calls([
        call(CONFIG_ENABLE_EVENT_LOG + "\n"),
        call(CONFIG_EVENT_LOG_DIR_FORMAT.format(EVENT_LOG_DIR) + "\n")
    ])

    src_file = EVENT_LOG_DIR + EVENT_FILE_NAME + EVENT_FILE_SUFFIX
    dst_file = SPARK_LOCAL_EVENT_LOG_DIR + EVENT_FILE_NAME
    mock_copy_file.assert_called_once_with(src_file, dst_file)
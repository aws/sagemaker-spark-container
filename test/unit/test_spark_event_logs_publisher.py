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
import os
import time
from unittest.mock import call, mock_open, patch

import pytest
from smspark.spark_event_logs_publisher import (
    CONFIG_ENABLE_EVENT_LOG,
    CONFIG_EVENT_LOG_DIR_FORMAT,
    EVENT_LOG_DIR,
    SPARK_DEFAULTS_CONFIG_PATH,
    SparkEventLogPublisher,
)

SPARK_LOCAL_EVENT_LOG_DIR = "spark/spark-events/"
SPARK_EVENT_LOG_S3_URI = "s3://bucket/folder"
TEST_FILE_CONTENT = "test"
TEST_WRITE_COUNT = 4
EVENT_FILE_NAME = "file_name"
EVENT_FILE_SUFFIX = ".inprogress"


@patch("smspark.spark_event_logs_publisher.copyfile")
@patch("os.makedirs")
@patch("os.listdir")
@patch("os.path.exists")
@patch("smspark.spark_event_logs_publisher.open", new_callable=mock_open)
def test_run_with_event_log_dir(mock_open_file, mock_os_path_exists, mock_os_listdir, mock_os_makedirs, mock_copy_file):
    mock_os_path_exists.return_value = False
    mock_os_listdir.side_effect = [[], [EVENT_FILE_NAME + EVENT_FILE_SUFFIX]]

    publisher = SparkEventLogPublisher(None, SPARK_LOCAL_EVENT_LOG_DIR, copy_interval=0.001)
    publisher.start()

    # For unit test purpose, sleep for 2 sec so while loop only run once
    time.sleep(0.1)
    publisher.down()
    publisher.join()

    mock_os_makedirs.assert_has_calls([call(EVENT_LOG_DIR), call(SPARK_LOCAL_EVENT_LOG_DIR)])

    mock_open_file.assert_called_with(SPARK_DEFAULTS_CONFIG_PATH, "a")

    mock_open_file().write.assert_has_calls(
        [call(CONFIG_ENABLE_EVENT_LOG + "\n"), call(CONFIG_EVENT_LOG_DIR_FORMAT.format(EVENT_LOG_DIR) + "\n"),]
    )

    src_file = EVENT_LOG_DIR + EVENT_FILE_NAME + EVENT_FILE_SUFFIX
    dst_file = SPARK_LOCAL_EVENT_LOG_DIR + EVENT_FILE_NAME
    mock_copy_file.assert_called_once_with(src_file, dst_file)


@patch("smspark.spark_event_logs_publisher.open", new_callable=mock_open)
def test_run_with_spark_events_s3_uri(mock_open_file):
    publisher = SparkEventLogPublisher(SPARK_EVENT_LOG_S3_URI, SPARK_LOCAL_EVENT_LOG_DIR, copy_interval=0.001)
    time.sleep(0.1)
    publisher.start()
    publisher.join()

    mock_open_file.assert_called_with(SPARK_DEFAULTS_CONFIG_PATH, "a")

    mock_open_file().write.assert_has_calls(
        [call(CONFIG_ENABLE_EVENT_LOG + "\n"), call(CONFIG_EVENT_LOG_DIR_FORMAT.format("s3a://bucket/folder") + "\n"),]
    )

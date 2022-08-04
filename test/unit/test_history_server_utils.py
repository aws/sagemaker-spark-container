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
import subprocess
from shutil import copyfile, rmtree
from unittest.mock import MagicMock, call, mock_open, patch

import pytest

from smspark.bootstrapper import Bootstrapper
from smspark.errors import InputError
from smspark.history_server_utils import (
    CONFIG_HISTORY_LOG_DIR_FORMAT,
    CONFIG_NOTEBOOK_PROXY_BASE,
    SPARK_DEFAULTS_CONFIG_PATH,
    config_history_server,
    start_history_server,
)

EVENT_LOGS_S3_URI = "s3://bucket/spark-events"


@patch("smspark.history_server_utils.open", new_callable=mock_open)
def test_config_history_server_with_env_variable(mock_open_file) -> None:
    config_history_server(EVENT_LOGS_S3_URI)

    mock_open_file.assert_called_with(SPARK_DEFAULTS_CONFIG_PATH, "a")
    mock_open_file().write.assert_has_calls(
        [
            call("spark.history.fs.logDirectory=s3://bucket/spark-events\n"),
            call("spark.ui.proxyBase=/proxy/15050\n"),
        ]
    )


def test_config_history_server_without_env_variable():
    with pytest.raises(InputError) as e:
        config_history_server(None)
    assert e.type == InputError


@patch("smspark.history_server_utils.config_history_server")
@patch("smspark.history_server_utils.Bootstrapper")
@patch("subprocess.run")
def test_start_history_server(mock_subprocess_run, mock_bootstrapper, mock_config_history_server) -> None:
    bootstrapper = MagicMock()
    mock_bootstrapper.return_value = bootstrapper
    start_history_server(SPARK_DEFAULTS_CONFIG_PATH)
    bootstrapper.start_spark_standalone_primary.assert_called_once()
    bootstrapper.copy_cluster_config.assert_called_once()
    bootstrapper.copy_aws_jars.assert_called_once()
    bootstrapper.set_regional_configs.assert_called_once()
    mock_config_history_server.assert_called_once()
    mock_subprocess_run.assert_called_once_with("sbin/start-history-server.sh", check=True)

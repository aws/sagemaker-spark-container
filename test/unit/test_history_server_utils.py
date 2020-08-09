import os
import pytest
import subprocess

from shutil import rmtree
from smspark import bootstrap
from shutil import copyfile
from unittest.mock import call
from unittest.mock import patch
from unittest.mock import mock_open
from smspark.history_server_utils import start_history_server
from smspark.history_server_utils import config_history_server
from smspark.history_server_utils import SPARK_DEFAULTS_CONFIG_PATH
from smspark.history_server_utils import CONFIG_HISTORY_LOG_DIR_FORMAT
from smspark.history_server_utils import CONFIG_NOTEBOOK_PROXY_BASE


@pytest.fixture(scope="function")
def notebook_instance_env_var() -> None:
    os.environ["SAGEMAKER_NOTEBOOK_INSTANCE_DOMAIN"] = "value"
    os.environ["SPARK_EVENT_LOGS_S3_URI"] = "s3://bucket/key"
    yield
    del os.environ['SAGEMAKER_NOTEBOOK_INSTANCE_DOMAIN']
    del os.environ["SPARK_EVENT_LOGS_S3_URI"]


@patch("smspark.history_server_utils.open", new_callable=mock_open)
def test_config_history_server_with_env_variable(mock_open_file,
                                                 notebook_instance_env_var):
    config_history_server()

    mock_open_file.assert_called_with(SPARK_DEFAULTS_CONFIG_PATH, "a")
    mock_open_file().write.assert_has_calls([
        call(CONFIG_HISTORY_LOG_DIR_FORMAT.format("s3a://bucket/key") + "\n"),
        call(CONFIG_NOTEBOOK_PROXY_BASE + "\n")
    ])


def test_config_history_server_without_env_variable():
    with pytest.raises(SystemExit) as e:
        config_history_server()

    assert e.type == SystemExit


@patch("smspark.history_server_utils.config_history_server")
@patch("smspark.bootstrap.copy_aws_jars")
@patch("smspark.bootstrap.copy_cluster_config")
@patch("smspark.bootstrap.start_primary")
@patch("subprocess.run")
def test_start_history_server(mock_subprocess_run,
                              mock_start_primary,
                              mock_copy_cluster_config,
                              mock_copy_aws_jars,
                              mock_config_history_server):

    start_history_server()

    mock_start_primary.assert_called_once()
    mock_copy_cluster_config.assert_called_once()
    mock_copy_aws_jars.assert_called_once()
    mock_config_history_server.assert_called_once()
    mock_subprocess_run.assert_called_once_with("sbin/start-history-server.sh", check=True, shell=True)
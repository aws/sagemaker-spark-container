import pytest

from dataclasses import dataclass
from typing import Type, Union
from unittest.mock import patch
from click.testing import CliRunner
from smspark.cli import submit
from smspark.job import ProcessingJobManager
from smspark.history_server_cli import run_history_server

ARGS_FORMAT = "--event-logs-s3-uri {} --remote-domain-name {}"
EVENT_LOGS_S3_URI = "s3://bucket"
REMOTE_DOMAIN_NAME = "domain"

@dataclass
class SubmitTest:
    """Represents data for one submit test."""

    name: str
    args: str
    expected_cmd: Union[str, Type[BaseException]]
    expected_s3_uri: str = None
    expected_local_dir: str = None

test_cases = [
        SubmitTest(
            name="When arguments are set, should be passed job manager",
            args="--spark-event-logs-s3-uri s3://bucket --local-spark-event-logs-dir /opt/ml/processing app.py",
            expected_cmd="spark-submit --master yarn --deploy-mode client app.py",
            expected_s3_uri="s3://bucket",
            expected_local_dir="/opt/ml/processing",
        ),
    ]

@patch("smspark.nginx_utils.start_nginx")
@patch("smspark.history_server_utils.start_history_server")
def test_run_history_server(mock_start_history_server, mock_start_nginx):
    runner = CliRunner()
    args = ARGS_FORMAT.format(EVENT_LOGS_S3_URI, REMOTE_DOMAIN_NAME)
    runner.invoke(run_history_server, args, standalone_mode=False)

    mock_start_nginx.assert_called_with(REMOTE_DOMAIN_NAME)
    mock_start_history_server.assert_called_with(EVENT_LOGS_S3_URI)


@patch("smspark.cli.ProcessingJobManager")
@pytest.mark.parametrize("test_case", test_cases, ids=[submit_test.name for submit_test in test_cases])
def test_submit(
    patched_processing_job_manager: ProcessingJobManager,
    test_case: SubmitTest,
) -> None:
    runner = CliRunner()

    result = runner.invoke(submit, test_case.args, standalone_mode=False)

    # happy
    if isinstance(test_case.expected_cmd, str):
        assert result.exception is None, result.output
        assert result.exit_code == 0
        patched_processing_job_manager.assert_called_once()
        patched_processing_job_manager.return_value.run.assert_called_once_with(test_case.expected_cmd,
                                                                                test_case.expected_s3_uri,
                                                                                test_case.expected_local_dir)

    # sad
    else:
        assert result.exit_code != 0, result.output
        assert isinstance(result.exception, test_case.expected_cmd)

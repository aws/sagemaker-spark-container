import os
from click.testing import CliRunner
from unittest.mock import patch
from smspark.history_server_cli import run_history_server

ARGS_FORMAT = "--event-logs-s3-uri {} --remote-domain-name {}"
EVENT_LOGS_S3_URI = "s3://bucket"
REMOTE_DOMAIN_NAME = "domain"

@patch("smspark.nginx_utils.start_nginx")
@patch("smspark.history_server_utils.start_history_server")
def test_run_history_server(mock_start_history_server,
                            mock_start_nginx):
    runner = CliRunner()
    args = ARGS_FORMAT.format(EVENT_LOGS_S3_URI, REMOTE_DOMAIN_NAME)
    runner.invoke(run_history_server, args, standalone_mode=False)

    mock_start_nginx.assert_called_with(REMOTE_DOMAIN_NAME)
    mock_start_history_server.assert_called_with(EVENT_LOGS_S3_URI)
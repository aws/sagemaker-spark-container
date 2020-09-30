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
from datetime import date
from unittest.mock import Mock, patch

import pytest
from requests import Session

from smspark.errors import AlgorithmError
from smspark.status import Status, StatusApp, StatusClient, StatusServer, _Clock


def test_status_app() -> None:
    def now_fn() -> date:
        return date(2000, 1, 1)

    app = StatusApp(status=Status.BOOTSTRAPPING, clock=_Clock(now_fn))

    resp = app(environ={}, start_response=lambda status, headers: None)

    assert len(resp) == 1
    assert resp[0] == b'{"status": "BOOTSTRAPPING", "timestamp": "2000-01-01"}'

    app.status = Status.WAITING
    resp = app(environ={}, start_response=lambda status, headers: None)

    assert len(resp) == 1
    assert resp[0] == b'{"status": "WAITING", "timestamp": "2000-01-01"}'


@patch("waitress.serve")
def test_status_server(mock_serve) -> None:
    def app(environ, start_response) -> None:
        return None

    server = StatusServer(app=app, hostname="algo-1")
    server.run()

    mock_serve.assert_called_once_with(app=app, listen="algo-1:5555")


@patch.object(Session, "get")
def test_status_map_one_host(mock_get) -> None:
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"status": "WAITING", "timestamp": "2000-01-01"}
    mock_get.return_value = mock_response

    status_map = StatusClient()
    status_message = status_map.get_status(["algo-1"])

    assert status_message["algo-1"].status == Status.WAITING


@patch.object(Session, "get")
def test_status_map_multiple_hosts(mock_get) -> None:
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"status": "WAITING", "timestamp": "2000-01-01"}
    mock_response.json.return_value = {"status": "WAITING", "timestamp": "2000-01-01"}
    mock_get.return_value = mock_response

    status_map = StatusClient()
    status_message = status_map.get_status(["algo-1", "algo-2"])

    assert status_message["algo-1"].status == Status.WAITING
    assert status_message["algo-2"].status == Status.WAITING


@patch.object(Session, "get")
def test_status_map_propagate_errors(mock_get) -> None:
    mock_get.side_effect = ValueError("Something went wrong")

    status_map = StatusClient()

    with pytest.raises(ValueError):
        status_map.get_status(["algo-1", "algo-2"])


@patch.object(Session, "get")
def test_status_map_http_error(mock_get) -> None:
    mock_response = Mock()
    mock_response.ok = False
    mock_get.return_value = mock_response
    status_map = StatusClient()
    with pytest.raises(AlgorithmError):
        status_map.get_status(["algo-1"])

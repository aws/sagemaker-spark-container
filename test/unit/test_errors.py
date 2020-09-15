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
import pathlib
from unittest.mock import mock_open, patch

from smspark.errors import AlgorithmError, BaseError


def test_algorithm_error() -> None:
    cause = ValueError("cause")
    error = AlgorithmError(message="message", caused_by=cause)

    failure_message = error.message
    assert failure_message == "Algorithm Error: (caused by ValueError): message: cause"


@patch("sys.exit")
@patch("smspark.errors.open", new_callable=mock_open)
@patch.object(pathlib.Path, "mkdir")
def test_exit(patched_path_mkdir, mock_open, patched_exit) -> None:
    class TestException(BaseException):
        pass

    error = BaseError(message="message", caused_by=TestException(), exit_code=5)

    error.log_and_exit()

    mock_open.assert_called_once_with("/opt/ml/output/message", "w"),
    handle = mock_open()
    handle.write.assert_called_once_with("Algorithm Error: (caused by TestException): message:")

    patched_exit.assert_called_once_with(5)

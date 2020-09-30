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
import time
from typing import Callable
from unittest.mock import patch

import pytest

from smspark.errors import AlgorithmError
from smspark.waiter import Waiter


def counter(invocation_count: int) -> Callable[..., int]:
    """Function that returns true after `invocation_count` invocations."""
    count = 0

    def inc() -> int:
        nonlocal count
        count += 1
        return count >= invocation_count

    return inc


@patch("time.time", return_value=1)
@patch("time.sleep")
def test_waiter(mock_time, mock_sleep) -> None:
    waiter = Waiter()

    waiter.wait_for(predicate_fn=counter(2), timeout=2.0, period=1.0)

    assert mock_sleep.call_count == 2


@patch("time.time", side_effect=range(0, 10))
@patch("time.sleep")
def test_waiter_timeout(mock_time, mock_sleep) -> None:
    waiter = Waiter()

    with pytest.raises(AlgorithmError):
        waiter.wait_for(predicate_fn=counter(4), timeout=2.0, period=1.0)


def test_waiter_pred_fn_errors() -> None:
    waiter = Waiter()

    def pred_fn() -> float:
        return 1 / 0

    with pytest.raises(TypeError):
        waiter.wait_for(pred_fn=pred_fn, timeout=2.0, callable=1.0)

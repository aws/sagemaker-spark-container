import time
from unittest.mock import patch

import pytest
from smspark.errors import AlgorithmError
from smspark.waiter import Waiter


def counter(invocation_count):
    """Function that returns true after `invocation_count` invocations."""
    count = 0

    def inc():
        nonlocal count
        count += 1
        return count >= invocation_count

    return inc


@patch("time.time", return_value=1)
@patch("time.sleep")
def test_waiter(mock_time, mock_sleep):
    waiter = Waiter()

    waiter.wait_for(predicate_fn=counter(2), timeout=2.0, period=1.0)

    assert mock_sleep.call_count == 2


@patch("time.time", side_effect=range(0, 10))
@patch("time.sleep")
def test_waiter_timeout(mock_time, mock_sleep):
    waiter = Waiter()

    with pytest.raises(AlgorithmError):
        waiter.wait_for(predicate_fn=counter(4), timeout=2.0, period=1.0)

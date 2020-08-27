"""Waiter implementation."""
import time
from typing import Callable

from smspark.errors import AlgorithmError


class Waiter:
    """Waits for a condition pred_fn to hold."""

    def wait_for(self, predicate_fn: Callable[..., bool], timeout: float, period: float) -> None:
        """Wait `timeout` seconds until `predicate_fn` returns `True`, polling every `period` seconds."""
        deadline = time.time() + timeout
        while not predicate_fn():
            time.sleep(period)
            if time.time() > deadline:
                raise AlgorithmError(
                    "Timed out waiting for function {}".format(predicate_fn.__name__), caused_by=TimeoutError(),
                )

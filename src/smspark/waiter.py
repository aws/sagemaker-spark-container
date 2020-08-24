import time
from typing import Callable

from smspark.errors import AlgorithmError


class Waiter:
    def wait_for(self, predicate_fn: Callable[..., bool], timeout: float, period: float) -> None:  # type: ignore
        """Wait `timeout` seconds until `predicate_fn` returns `True`, polling every `period` seconds."""
        deadline = time.time() + timeout
        while not predicate_fn():
            time.sleep(period)
            if time.time() > deadline:
                raise AlgorithmError("Timed out waiting for function {}".format(predicate_fn.__name__))

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

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from enum import Enum
from threading import Thread
from typing import Callable, Dict, Iterable, Mapping, Sequence

import requests
import waitress
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from smspark.errors import AlgorithmError


class Status(str, Enum):
    """Enum of host statuses."""

    INITIALIZING = "INITIALIZING"
    BOOTSTRAPPING = "BOOTSTRAPPING"
    WAITING = "WAITING"

    def __repr__(self) -> str:
        return '"{}"'.format(self.name)

    def __str__(self) -> str:
        return "{}".format(self.name)


@dataclass
class StatusMessage:
    """ Response message containing information about a host's status. For example,

        {"status": "WAITING", "timestamp": "2020-08-01T01:23:45.56789"}
    """

    status: Status
    timestamp: str


class StatusClient:
    """Gets the status for lists of hosts"""

    async def _get_host_statuses(self, hosts: Iterable[str]) -> Iterable[StatusMessage]:
        async def get_host_status(host: str) -> StatusMessage:
            s = requests.Session()
            retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
            s.mount("http://", HTTPAdapter(max_retries=retries))
            url = "http://{}:{}".format(host, StatusServer.port)
            try:
                resp = s.get(url, timeout=1.0)
                if resp.ok:
                    status_message = StatusMessage(**resp.json())
                    return status_message
                else:
                    raise AlgorithmError(
                        message="Could not get status for host {} status code: {} response: {}".format(
                            host, resp.status_code, resp.text
                        )
                    )
            except Exception as e:
                raise AlgorithmError(message="Exception while getting status for host {}".format(host), caused_by=e)

        tasks = [asyncio.create_task(get_host_status(host)) for host in hosts]
        return await asyncio.gather(*tasks)

    def get_status(self, hosts: Iterable[str]) -> Mapping[str, StatusMessage]:
        """Return a mapping from hostname to StatusMessage with that host's status."""
        statuses = asyncio.run(self._get_host_statuses(hosts))
        return dict(zip(hosts, statuses))


class _Clock:
    """Stub for datetime.datetime.now().

    This exists because attributes on datetime.datetime can't be patched."""

    def __init__(self, now_fn: Callable[[], date] = lambda: datetime.now()):
        self._now_fn = now_fn

    def now(self) -> date:
        return self._now_fn()


class StatusApp:
    """A WSGI application that allows hosts to ask each other for their status.

    For example:
    * the primary shouldn't run spark-submit until the worker nodes are waiting
    * the workers shouldn't exit until the primary has exited.
    """

    _clock = _Clock()

    def __init__(self, status: Status = Status.INITIALIZING, clock: _Clock = _clock):
        self._status = status
        self._clock = clock
        self.logger = logging.getLogger("smspark-submit")

    def __call__(self, environ: Dict[str, str], start_response: Callable) -> Sequence[bytes]:  # type: ignore
        """Handle GET requests to /, responding with a JSON `StatusMessage`."""
        status = "200 OK"
        headers = [("Content-type", "text/plain; charset=utf-8")]

        start_response(status, headers)
        timestamp = self._clock.now().isoformat()
        payload = json.dumps(asdict(StatusMessage(status=self._status, timestamp=timestamp)))

        return [payload.encode("utf-8")]

    @property
    def status(self) -> Status:
        return self._status

    @status.setter
    def status(self, new_status: Status) -> None:
        self.logger.info("transitioning from status {} to {}".format(self._status, new_status))
        self._status = new_status


class StatusServer(Thread):

    port = 5555

    def __init__(self, app: Callable, hostname: str):  # type: ignore
        Thread.__init__(self)
        self.logger = logging.getLogger("smspark-submit")
        self.app = app
        self.hostname = hostname

    def run(self) -> None:
        """Runs a WSGI server in a thread"""
        addr = "{}:{}".format(self.hostname, StatusServer.port)
        self.logger.info("Status server listening on {}".format(addr))
        waitress.serve(app=self.app, listen="{}".format(addr))

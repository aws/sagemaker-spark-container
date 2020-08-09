import logging

import click
from smspark import history_server_utils, nginx_utils

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command(context_settings=dict(allow_interspersed_args=False))
def run_history_server() -> None:
    """Run the Spark History Server."""
    log.info("Running smspark-server")

    nginx_utils.start_nginx()
    log.info("Running spark history server")
    history_server_utils.start_history_server()

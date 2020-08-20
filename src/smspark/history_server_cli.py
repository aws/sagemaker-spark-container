import logging

import click
from smspark import history_server_utils, nginx_utils

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command(context_settings=dict(allow_interspersed_args=False))
@click.option("--event-logs-s3-uri", required=True, help="S3 uri stores spark events that history server can read from")
@click.option("--remote-domain-name", help="Domain name of remote device when history server is running remotely")
@click.pass_context
def run_history_server(ctx, event_logs_s3_uri, remote_domain_name) -> None:
    """Run the Spark History Server."""
    nginx_utils.start_nginx(remote_domain_name)
    log.info("Running spark history server")
    history_server_utils.start_history_server(event_logs_s3_uri)

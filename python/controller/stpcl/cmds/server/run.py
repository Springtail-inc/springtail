import click
import logging

from ..cli import server

logger = logging.Logger("stpcl.server.run")


@server.command("run")
@click.option(
    "--host",
    type=str,
    required=False,
    default="0.0.0.0",
    help="Host to bind the server. Default: '0.0.0.0'.",
)
@click.option(
    "--port",
    type=int,
    required=False,
    default=8000,
    help="Port to bind the server. Default: 8000.",
)
@click.option(
    "--workers",
    type=int,
    required=False,
    default=2,
    help="Number of worker processes. Default: 2.",
)
@click.pass_obj
def run(
        obj,
        host: str,
        port: int,
        workers: int,
):
    from stpcl.imp.server import run_server
    run_server(host, port, workers=workers)

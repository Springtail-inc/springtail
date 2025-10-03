import click
from click import pass_obj

from ..cli import (
    config,
)

MODE_PRIMARY = "primary"
MODE_SHADOW = "shadow"
MODE_NORMAL = "normal"


@config.command("set-proxy-mode")
@click.argument("mode", type=click.Choice([MODE_PRIMARY, MODE_SHADOW, MODE_NORMAL], case_sensitive=False))
@pass_obj
def set_proxy_mode(obj, mode):
    """Set the proxy mode for the application.

    MODE can be one of the following:
    - primary: All traffic is routed to primary
    - shadow: Traffic is forwarded to both primary and fdw and logs the responses from primary.
    - normal: Split traffic between primary and fdw based on the read/write rules.
    """
    from stpcl.imp.config import (
        Config,
    )
    redis = obj["redis"]

    instance_id = obj["instance_id"]

    cfg = Config(instance_id, redis)

    o = cfg.instance_config.system_settings

    o["proxy"]["mode"] = mode

    cfg.instance_config.system_settings = o

    docker_cli = obj["docker_cli"]
    docker_cli.exec_systemctl_in_container("proxy", "restart", "springtail-coordinator")

    click.echo(f"Proxy mode set to {mode}.")

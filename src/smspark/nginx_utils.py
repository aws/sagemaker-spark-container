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
"""Functions to write and configure NGINX."""
import logging
import subprocess
from shutil import copyfile
from typing import Optional

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%m-%d %H:%M",
)
log = logging.getLogger("sagemaker-spark")

NGINX_ENV_VARIABLE_CONFIG_FORMAT = "map $host $domain_name {{default {};}}"

# Running locally with --network host, the history server will be running on 0.0.0.0
# rather than localhost
DOMAIN_LOCALHOST = "http://0.0.0.0"

# Dockfile copies the local config file to /opt/nginx-config first. To successfully run
# nginx, start_nginx() modifies the files in /opt/nginx-config and copy to nginx dir(/etc/nginx/)
# /etc/nginx/nginx.conf is the entry config file for nginx, /etc/nginx/conf.d/default.conf is
# a script included in nginx.conf, which usually has all servers listening on different port
NGINX_CONTAINER_DEFAULT_CONFIG_PATH = "/opt/nginx-config/default.conf"
NGINX_DEFAULT_CONFIG_PATH = "/etc/nginx/conf.d/default.conf"
NGINX_CONTAINER_CONFIG_PATH = "/opt/nginx-config/nginx.conf"
NGINX_CONFIG_PATH = "/etc/nginx/nginx.conf"


def start_nginx(remote_domain_name: str) -> None:
    """Start an nginx instance with the given remote domain name."""
    copy_nginx_default_conf()
    write_nginx_default_conf(remote_domain_name)

    logging.info("Starting nginx.")
    subprocess.run("/usr/sbin/nginx -c /etc/nginx/nginx.conf", shell=True)


def write_nginx_default_conf(remote_domain_name: Optional[str]) -> None:
    """Inject NGINX config value and write NGINX config file.

    When running spark history behind notebook proxy, the domain will change because of
    the redirect behavior of spark itself, which ignores the proxy. Nginx can't easily
    support env variable. Here we inject env variable to the default.conf file.
    """
    if remote_domain_name is not None:
        with open(NGINX_DEFAULT_CONFIG_PATH, "a") as ngxin_conf:
            ngxin_conf.write(NGINX_ENV_VARIABLE_CONFIG_FORMAT.format(remote_domain_name))
    else:
        with open(NGINX_DEFAULT_CONFIG_PATH, "a") as ngxin_conf:
            ngxin_conf.write(NGINX_ENV_VARIABLE_CONFIG_FORMAT.format(DOMAIN_LOCALHOST))


def copy_nginx_default_conf() -> None:
    """Copy NGINX config files from container paths to NGINX config paths."""
    logging.info("copying {} to {}".format(NGINX_CONTAINER_DEFAULT_CONFIG_PATH, NGINX_DEFAULT_CONFIG_PATH))
    copyfile(NGINX_CONTAINER_DEFAULT_CONFIG_PATH, NGINX_DEFAULT_CONFIG_PATH)

    logging.info("copying {} to {}".format(NGINX_CONTAINER_CONFIG_PATH, NGINX_CONFIG_PATH))
    copyfile(NGINX_CONTAINER_CONFIG_PATH, NGINX_CONFIG_PATH)

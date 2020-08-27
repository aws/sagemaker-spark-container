from unittest.mock import call, mock_open, patch

from smspark.nginx_utils import (
    DOMAIN_LOCALHOST,
    NGINX_CONFIG_PATH,
    NGINX_CONTAINER_CONFIG_PATH,
    NGINX_CONTAINER_DEFAULT_CONFIG_PATH,
    NGINX_DEFAULT_CONFIG_PATH,
    NGINX_ENV_VARIABLE_CONFIG_FORMAT,
    copy_nginx_default_conf,
    start_nginx,
    write_nginx_default_conf,
)

REMOTE_DOMAIN_NAME = "http://domain.com"


@patch("smspark.nginx_utils.copy_nginx_default_conf")
@patch("smspark.nginx_utils.write_nginx_default_conf")
@patch("subprocess.run")
def test_start_nginx(mock_subprocess_run, mock_write_nginx_default_conf, mock_copy_nginx_default_conf):
    start_nginx(REMOTE_DOMAIN_NAME)

    mock_copy_nginx_default_conf.assert_called_once()
    mock_write_nginx_default_conf.assert_called_once_with(REMOTE_DOMAIN_NAME)
    mock_subprocess_run.assert_called_once_with("/usr/sbin/nginx -c /etc/nginx/nginx.conf", shell=True)


@patch("smspark.nginx_utils.open", new_callable=mock_open)
def test_write_nginx_default_conf(mock_open_file):
    write_nginx_default_conf(REMOTE_DOMAIN_NAME)

    mock_open_file.assert_called_with(NGINX_DEFAULT_CONFIG_PATH, "a")
    mock_open_file().write.assert_called_with(NGINX_ENV_VARIABLE_CONFIG_FORMAT.format(REMOTE_DOMAIN_NAME))


@patch("smspark.nginx_utils.open", new_callable=mock_open)
def test_write_nginx_default_conf_without_domain_name(mock_open_file):
    write_nginx_default_conf("http://0.0.0.0")

    mock_open_file.assert_called_with(NGINX_DEFAULT_CONFIG_PATH, "a")
    mock_open_file().write.assert_called_with(NGINX_ENV_VARIABLE_CONFIG_FORMAT.format(DOMAIN_LOCALHOST))


@patch("smspark.nginx_utils.copyfile")
def test_copy_nginx_default_conf(mock_copy_file):
    copy_nginx_default_conf()

    mock_copy_file.assert_has_calls(
        [
            call(NGINX_CONTAINER_DEFAULT_CONFIG_PATH, NGINX_DEFAULT_CONFIG_PATH),
            call(NGINX_CONTAINER_CONFIG_PATH, NGINX_CONFIG_PATH),
        ]
    )

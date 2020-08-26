from unittest.mock import MagicMock, Mock, call, patch

import pytest
from smspark.bootstrapper import Bootstrapper
from smspark.config import Configuration
from smspark.defaults import default_resource_config


@pytest.fixture
def default_bootstrapper() -> Bootstrapper:
    return Bootstrapper(default_resource_config)


def test_recursive_deserialize_user_configuration(default_bootstrapper):
    test_case = [
        {
            "Classification": "core-site",
            "Properties": {"prop": "value"},
            "Configurations": [{"Classification": "export", "Properties": {"inner-prop": "inner-value"}}],
        }
    ]

    expected = [
        Configuration(
            "core-site", {"prop": "value"}, Configurations=[Configuration("export", {"inner-prop": "inner-value"})]
        )
    ]
    output = default_bootstrapper.deserialize_user_configuration(test_case)

    assert output == expected


def test_site_multiple_classifications(default_bootstrapper):
    list_of_configurations = [
        {"Classification": "core-site", "Properties": {"hadoop.security.groups.cache.secs": "250"}},
        {
            "Classification": "yarn-site",
            "Properties": {
                "mapred.tasktracker.map.tasks.maximum": "2",
                "mapreduce.map.sort.spill.percent": "0.90",
                "mapreduce.tasktracker.reduce.tasks.maximum": "5",
            },
        },
    ]

    output = default_bootstrapper.deserialize_user_configuration(list_of_configurations)
    expected = [
        Configuration("core-site", {"hadoop.security.groups.cache.secs": "250"}),
        Configuration(
            "yarn-site",
            {
                "mapred.tasktracker.map.tasks.maximum": "2",
                "mapreduce.map.sort.spill.percent": "0.90",
                "mapreduce.tasktracker.reduce.tasks.maximum": "5",
            },
        ),
    ]

    assert output == expected


def test_env_classification(default_bootstrapper):
    hadoop_env_conf = {
        "Classification": "hadoop-env",
        "Properties": {},
        "Configurations": [
            {
                "Classification": "export",
                "Properties": {"HADOOP_DATANODE_HEAPSIZE": "2048", "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"},
            }
        ],
    }

    output = default_bootstrapper.deserialize_user_configuration(hadoop_env_conf)
    expected = Configuration(
        "hadoop-env",
        {},
        Configurations=[
            Configuration("export", {"HADOOP_DATANODE_HEAPSIZE": "2048", "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"})
        ],
    )

    assert output == expected


@patch("glob.glob", side_effect=[["/aws-sdk.jar"], ["/hmclient/lib/client.jar"]])
@patch("shutil.copyfile", side_effect=None)
def test_copy_aws_jars(patched_copyfile, patched_glob, default_bootstrapper) -> None:
    default_bootstrapper.copy_aws_jars()

    expected = [
        call("/aws-sdk.jar", "/usr/lib/spark/jars/aws-sdk.jar"),
        call("/usr/lib/hadoop/hadoop-aws-2.8.5-amzn-5.jar", "/usr/lib/spark/jars/hadoop-aws-2.8.5-amzn-5.jar"),
        call("/usr/lib/hadoop/jets3t-0.9.0.jar", "/usr/lib/spark/jars/jets3t-0.9.0.jar"),
        call("/hmclient/lib/client.jar", "/usr/lib/spark/jars/client.jar"),
    ]
    patched_copyfile.call_args_list == expected


def test_bootstrap_smspark_submit(default_bootstrapper) -> None:
    default_bootstrapper.copy_aws_jars = MagicMock()
    default_bootstrapper.copy_cluster_config = MagicMock()
    default_bootstrapper.write_runtime_cluster_config = MagicMock()
    default_bootstrapper.write_user_configuration = MagicMock()
    default_bootstrapper.start_hadoop_daemons = MagicMock()
    default_bootstrapper.wait_for_hadoop = MagicMock()

    default_bootstrapper.bootstrap_smspark_submit()

    default_bootstrapper.copy_aws_jars.assert_called_once()
    default_bootstrapper.copy_cluster_config.assert_called_once()
    default_bootstrapper.write_runtime_cluster_config.assert_called_once()
    default_bootstrapper.write_user_configuration.assert_called_once()
    default_bootstrapper.start_hadoop_daemons.assert_called_once()
    default_bootstrapper.wait_for_hadoop.assert_called_once()


def test_bootstrap_history_server(default_bootstrapper) -> None:
    default_bootstrapper.copy_aws_jars = MagicMock()
    default_bootstrapper.copy_cluster_config = MagicMock()
    default_bootstrapper.start_spark_standalone_primary = MagicMock()

    default_bootstrapper.bootstrap_history_server()

    default_bootstrapper.copy_aws_jars.assert_called_once()
    default_bootstrapper.copy_cluster_config.assert_called_once()
    default_bootstrapper.start_spark_standalone_primary.assert_called_once()


@patch("requests.get")
def test_wait_for_hadoop(mock_get, default_bootstrapper) -> None:
    mock_response = Mock()
    mock_response.ok = True
    mock_get.return_value = mock_response
    default_bootstrapper.wait_for_hadoop()


@patch("shutil.copyfile", side_effect=None)
def test_copy_cluster_config(patched_copyfile, default_bootstrapper) -> None:
    default_bootstrapper.copy_cluster_config()

    expected = [
        call("/opt/hadoop-config/hdfs-site.xml", "/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"),
        call("/opt/hadoop-config/core-site.xml", "/usr/lib/hadoop/etc/hadoop/core-site.xml"),
        call("/opt/hadoop-config/yarn-site.xml", "/usr/lib/hadoop/etc/hadoop/yarn-site.xml"),
        call("/opt/hadoop-config/spark-defaults.conf", "/usr/lib/hadoop/conf/spark-defaults.conf"),
        call("/opt/hadoop-config/spark-env.sh", "/usr/lib/hadoop/conf/spark-env.sh"),
    ]

    patched_copyfile.call_args_list == expected


@patch("subprocess.Popen")
@patch("subprocess.call")
def test_start_hadoop_daemons_on_primary(patched_popen, patched_call, default_bootstrapper) -> None:
    default_bootstrapper.start_hadoop_daemons()

    expected_subprocess_calls = [
        call("rm -rf /opt/amazon/hadoop/hdfs/namenode && mkdir -p /opt/amazon/hadoop/hdfs/namenode", shell=True),
        call("rm -rf /opt/amazon/hadoop/hdfs/datanode && mkdir -p /opt/amazon/hadoop/hdfs/datanode", shell=True),
        call("hdfs namenode -format -force", shell=True),
    ]

    patched_call.call_args_list = expected_subprocess_calls

    expected_subprocess_popens = [
        call("hdfs namenode", shell=True),
        call("hdfs datanode", shell=True),
        call("yarn resourcemanager", shell=True),
        call("yarn nodemanager", shell=True),
    ]

    patched_popen.call_args_list == expected_subprocess_popens


@patch("subprocess.Popen")
@patch("subprocess.call")
def test_start_hadoop_daemons_on_worker(patched_popen, patched_call) -> None:
    worker_bootstrapper = Bootstrapper(resource_config={"current_host": "algo-2", "hosts": ["algo-1", "algo-2"]})
    worker_bootstrapper.start_hadoop_daemons()

    expected_subprocess_calls = [
        call("rm -rf /opt/amazon/hadoop/hdfs/datanode && mkdir -p /opt/amazon/hadoop/hdfs/datanode", shell=True),
    ]

    patched_call.call_args_list = expected_subprocess_calls

    expected_subprocess_popens = [
        call("hdfs datanode", shell=True),
        call("yarn nodemanager", shell=True),
    ]

    patched_popen.call_args_list == expected_subprocess_popens


@patch("subprocess.Popen")
def test_spark_standalone_primary(patched_popen, default_bootstrapper) -> None:

    default_bootstrapper.start_spark_standalone_primary()

    patched_popen.assert_called_once_with("/usr/lib/spark/sbin/start-master.sh", shell=True)

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
import json
from unittest.mock import MagicMock, Mock, PropertyMock, call, mock_open, patch

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
            "core-site", {"prop": "value"}, Configurations=[Configuration("export", {"inner-prop": "inner-value"})],
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
                "Properties": {"HADOOP_DATANODE_HEAPSIZE": "2048", "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19",},
            }
        ],
    }

    output = default_bootstrapper.deserialize_user_configuration(hadoop_env_conf)
    expected = Configuration(
        "hadoop-env",
        {},
        Configurations=[
            Configuration("export", {"HADOOP_DATANODE_HEAPSIZE": "2048", "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"},)
        ],
    )

    assert output == expected


@patch("glob.glob", side_effect=[["/aws-sdk.jar"], ["/hmclient/lib/client.jar"]])
@patch("shutil.copyfile", side_effect=None)
def test_copy_aws_jars(patched_copyfile, patched_glob, default_bootstrapper) -> None:
    default_bootstrapper.copy_aws_jars()

    expected = [
        call("/aws-sdk.jar", "/usr/lib/spark/jars/aws-sdk.jar"),
        call("/usr/lib/hadoop/hadoop-aws-2.8.5-amzn-5.jar", "/usr/lib/spark/jars/hadoop-aws-2.8.5-amzn-5.jar",),
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
        call("rm -rf /opt/amazon/hadoop/hdfs/namenode && mkdir -p /opt/amazon/hadoop/hdfs/namenode", shell=True,),
        call("rm -rf /opt/amazon/hadoop/hdfs/datanode && mkdir -p /opt/amazon/hadoop/hdfs/datanode", shell=True,),
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
        call("rm -rf /opt/amazon/hadoop/hdfs/datanode && mkdir -p /opt/amazon/hadoop/hdfs/datanode", shell=True,),
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


@patch("smspark.config.Configuration")
def test_set_regional_configs(patched_config, default_bootstrapper: Bootstrapper) -> None:
    default_bootstrapper.get_regional_configs = MagicMock(return_value=[patched_config])
    default_bootstrapper.set_regional_configs()
    default_bootstrapper.get_regional_configs.assert_called_once()
    patched_config.write_config.assert_called_once()


@patch("smspark.config.Configuration")
def test_set_regional_configs_empty(patched_config, default_bootstrapper: Bootstrapper) -> None:
    default_bootstrapper.get_regional_configs = MagicMock(return_value=[])
    default_bootstrapper.set_regional_configs()
    default_bootstrapper.get_regional_configs.assert_called_once()
    patched_config.write_config.assert_not_called()


@patch("os.getenv")
def test_get_regional_configs_cn(patched_getenv, default_bootstrapper: Bootstrapper) -> None:
    patched_getenv.return_value = "cn-northwest-1"
    regional_configs_list = default_bootstrapper.get_regional_configs()
    assert len(regional_configs_list) == 1
    assert regional_configs_list[0] == Configuration(
        Classification="core-site", Properties={"fs.s3a.endpoint": "s3.cn-northwest-1.amazonaws.com.cn"}
    )
    patched_getenv.assert_called_once_with("AWS_REGION")


@patch("os.getenv")
def test_get_regional_configs_gov(patched_getenv, default_bootstrapper: Bootstrapper) -> None:
    patched_getenv.return_value = "us-gov-west-1"
    regional_configs_list = default_bootstrapper.get_regional_configs()
    assert len(regional_configs_list) == 1
    assert regional_configs_list[0] == Configuration(
        Classification="core-site", Properties={"fs.s3a.endpoint": "s3.us-gov-west-1.amazonaws.com"}
    )
    patched_getenv.assert_called_once_with("AWS_REGION")


@patch("os.getenv")
def test_get_regional_configs_us(patched_getenv, default_bootstrapper: Bootstrapper) -> None:
    patched_getenv.return_value = "us-west-2"
    regional_configs_list = default_bootstrapper.get_regional_configs()
    assert len(regional_configs_list) == 0
    patched_getenv.assert_called_once_with("AWS_REGION")


@patch("os.getenv")
def test_get_regional_configs_missing_region(patched_getenv, default_bootstrapper: Bootstrapper) -> None:
    patched_getenv.return_value = None
    regional_configs_list = default_bootstrapper.get_regional_configs()
    assert len(regional_configs_list) == 0
    patched_getenv.assert_called_once_with("AWS_REGION")


@patch("os.path.exists")
def test_load_processing_job_config(patched_exists, default_bootstrapper: Bootstrapper) -> None:
    exp_config = {"ProcessingResources": {"ClusterConfig": {"InstanceType": "foo.xbar", "InstanceCount": 123}}}

    patched_exists.return_value = True
    with patch("smspark.bootstrapper.open", mock_open(read_data=json.dumps(exp_config))) as m:
        actual_config = default_bootstrapper.load_processing_job_config()
    assert actual_config == exp_config
    patched_exists.assert_called_once_with(Bootstrapper.PROCESSING_JOB_CONFIG_PATH)
    m.assert_called_once_with(Bootstrapper.PROCESSING_JOB_CONFIG_PATH, "r")


@patch("os.path.exists")
def test_load_processing_job_config_fallback(patched_exists, default_bootstrapper: Bootstrapper) -> None:
    patched_exists.return_value = False
    assert default_bootstrapper.load_processing_job_config() == {}
    patched_exists.assert_called_once_with(Bootstrapper.PROCESSING_JOB_CONFIG_PATH)


@patch("os.path.exists")
def test_load_instance_type_info(patched_exists, default_bootstrapper: Bootstrapper) -> None:
    raw_config = [
        {"InstanceType": "foo.xlarge", "foo": "bar"},
        {"InstanceType": "bar.xlarge", "bar": "foo",},
    ]
    exp_config = {"foo.xlarge": {"foo": "bar"}, "bar.xlarge": {"bar": "foo"}}

    patched_exists.return_value = True
    with patch("smspark.bootstrapper.open", mock_open(read_data=json.dumps(raw_config))) as m:
        actual_config = default_bootstrapper.load_instance_type_info()
    assert actual_config == exp_config
    patched_exists.assert_called_once_with(Bootstrapper.INSTANCE_TYPE_INFO_PATH)
    m.assert_called_once_with(Bootstrapper.INSTANCE_TYPE_INFO_PATH, "r")


@patch("os.path.exists")
def test_load_instance_type_info(patched_exists, default_bootstrapper: Bootstrapper) -> None:
    patched_exists.return_value = False
    assert default_bootstrapper.load_instance_type_info() == {}
    patched_exists.assert_called_once_with(Bootstrapper.INSTANCE_TYPE_INFO_PATH)


@patch("smspark.config.Configuration")
@patch("smspark.config.Configuration")
def test_set_yarn_spark_resource_config(
    patched_yarn_config, patched_spark_config, default_bootstrapper: Bootstrapper
) -> None:
    processing_job_config = {
        "ProcessingResources": {"ClusterConfig": {"InstanceType": "foo.xbar", "InstanceCount": 123}}
    }
    instance_type_info = {"foo.xbar": {"MemoryInfo": {"SizeInMiB": 456}, "VCpuInfo": {"DefaultVCpus": 789}}}
    default_bootstrapper.load_processing_job_config = MagicMock(return_value=processing_job_config)
    default_bootstrapper.load_instance_type_info = MagicMock(return_value=instance_type_info)
    default_bootstrapper.get_yarn_spark_resource_config = MagicMock(
        return_value=(patched_yarn_config, patched_spark_config)
    )

    default_bootstrapper.set_yarn_spark_resource_config()

    default_bootstrapper.load_processing_job_config.assert_called_once()
    default_bootstrapper.load_instance_type_info.assert_called_once()
    default_bootstrapper.get_yarn_spark_resource_config.assert_called_once_with(123, 456, 789)
    patched_yarn_config.write_config.assert_called_once()
    patched_spark_config.write_config.assert_called_once()


@patch("smspark.config.Configuration")
@patch("smspark.config.Configuration")
@patch("psutil.cpu_count")
@patch("psutil.virtual_memory")
def test_set_yarn_spark_resource_config_fallback(
    patched_virtual_memory,
    patched_cpu_count,
    patched_yarn_config,
    patched_spark_config,
    default_bootstrapper: Bootstrapper,
) -> None:
    mocked_virtual_memory_total = PropertyMock(return_value=123 * 1024 * 1024)
    type(patched_virtual_memory.return_value).total = mocked_virtual_memory_total
    patched_cpu_count.return_value = 456

    default_bootstrapper.load_processing_job_config = MagicMock(return_value=None)
    default_bootstrapper.load_instance_type_info = MagicMock(return_value=None)
    default_bootstrapper.get_yarn_spark_resource_config = MagicMock(
        return_value=(patched_yarn_config, patched_spark_config)
    )

    default_bootstrapper.set_yarn_spark_resource_config()

    patched_virtual_memory.assert_called_once()
    mocked_virtual_memory_total.assert_called_once()
    patched_cpu_count.assert_called_once()

    default_bootstrapper.load_processing_job_config.assert_called_once()
    default_bootstrapper.load_instance_type_info.assert_called_once()
    default_bootstrapper.get_yarn_spark_resource_config.assert_called_once_with(1, 123, 456)
    patched_yarn_config.write_config.assert_called_once()
    patched_spark_config.write_config.assert_called_once()


def test_get_yarn_spark_resource_config(default_bootstrapper: Bootstrapper) -> None:
    # Using a cluster with one single m5.xlarge instance, calculate Yarn and Spark configs, and double check the math
    instance_mem_mb = 16384
    instance_cores = 4
    yarn_config, spark_config = default_bootstrapper.get_yarn_spark_resource_config(1, instance_mem_mb, instance_cores)

    exp_yarn_max_mem_mb = 15892  # = int(instance_mem_mb * .97) = int(16384 * .97) = int(15892.48)

    exp_yarn_config_props = {
        "yarn.scheduler.minimum-allocation-mb": "1",
        "yarn.scheduler.maximum-allocation-mb": str(exp_yarn_max_mem_mb),
        "yarn.scheduler.minimum-allocation-vcores": "1",
        "yarn.scheduler.maximum-allocation-vcores": str(instance_cores),
        "yarn.nodemanager.resource.memory-mb": str(exp_yarn_max_mem_mb),
        "yarn.nodemanager.resource.cpu-vcores": str(instance_cores),
    }

    assert yarn_config.Classification == "yarn-site"
    assert yarn_config.Properties == exp_yarn_config_props

    exp_executor_cores = 4  # = instance_cores = 4
    exp_executor_count_total = 1  # = instance_count * executor_count_per_instance = 1 * 1
    exp_default_parallelism = 8  # = instance_count * instance_cores * 2 = 1 * 4 * 2

    exp_driver_mem_mb = 2048  # = 2 * 1024
    exp_driver_mem_ovr_mb = 204  # = int(driver_mem_mb * driver_mem_ovr_pct) = int(2048 * 0.1) = int(204.8)
    # = int((instance_mem_mb - driver_mem_mb - driver_mem_ovr_mb) /
    #       (executor_count_per_instance + executor_count_per_instance * executor_mem_ovr_pct))
    # = int((15892 - 2048 - 204) / (1 + 1 * 0.1))
    # = int(13640 / 1.1)
    exp_executor_mem_mb = 12399
    exp_executor_mem_ovr_mb = 1239  # = int(executor_mem_mb * executor_mem_ovr_pct) = int(12399 * 0.1) = int(1239.9)

    exp_driver_gc_config = (
        "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 "
        "-XX:+CMSClassUnloadingEnabled"
    )
    exp_driver_java_opts = "-XX:OnOutOfMemoryError='kill -9 %p' " f"{exp_driver_gc_config}"

    # ConcGCThreads = max(int(executor_cores / 4), 1) = max(int(4 / 4), 1) = max(1, 1) = 1
    # ParallelGCThreads = max(int(3 * executor_cores / 4), 1) = max(int(3 * 4 / 4), 1) = max(3, 1) = 3
    exp_executor_gc_config = (
        "-XX:+UseParallelGC -XX:InitiatingHeapOccupancyPercent=70 " "-XX:ConcGCThreads=1 " "-XX:ParallelGCThreads=3 "
    )
    exp_executor_java_opts = (
        "-verbose:gc -XX:OnOutOfMemoryError='kill -9 %p' "
        "-XX:+PrintGCDetails -XX:+PrintGCDateStamps "
        f"{exp_executor_gc_config}"
    )

    exp_spark_config_props = {
        "spark.driver.memory": f"{exp_driver_mem_mb}m",
        "spark.driver.memoryOverhead": f"{exp_driver_mem_ovr_mb}m",
        "spark.driver.defaultJavaOptions": f"{exp_driver_java_opts}m",
        "spark.executor.memory": f"{exp_executor_mem_mb}m",
        "spark.executor.memoryOverhead": f"{exp_executor_mem_ovr_mb}m",
        "spark.executor.cores": f"{exp_executor_cores}",
        "spark.executor.defaultJavaOptions": f"{exp_executor_java_opts}",
        "spark.executor.instances": f"{exp_executor_count_total}",
        "spark.default.parallelism": f"{exp_default_parallelism}",
    }

    assert spark_config.Classification == "spark-defaults"
    assert spark_config.Properties == exp_spark_config_props

    # Using the same instance type, increase the instance count by 10x
    yarn_config, spark_config = default_bootstrapper.get_yarn_spark_resource_config(10, instance_mem_mb, instance_cores)

    # Yarn config should be the same
    assert yarn_config.Properties == exp_yarn_config_props

    # Spark config should be the same with more 10x executors and parallelism
    exp_spark_config_props["spark.executor.instances"] = f"{exp_executor_count_total * 10}"
    exp_spark_config_props["spark.default.parallelism"] = f"{exp_default_parallelism * 10}"
    assert spark_config.Properties == exp_spark_config_props

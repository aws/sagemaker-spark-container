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
# flake8: noqa
import errno
import glob
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import psutil
import requests
from smspark.config import Configuration
from smspark.defaults import default_resource_config
from smspark.errors import AlgorithmError
from smspark.waiter import Waiter


class Bootstrapper:
    """Initializes the cluster."""

    NODEMANAGER_WEBAPP_ADDR_PORT = 8042

    HADOOP_CONFIG_PATH = "/opt/hadoop-config/"
    HADOOP_PATH = "/usr/lib/hadoop"
    SPARK_PATH = "/usr/lib/spark"

    HIVE_PATH = "/usr/lib/hive"
    PROCESSING_CONF_INPUT_PATH = "/opt/ml/processing/input/conf/configuration.json"
    PROCESSING_JOB_CONFIG_PATH = "/opt/ml/config/processingjobconfig.json"
    INSTANCE_TYPE_INFO_PATH = "/opt/aws-config/ec2-instance-type-info.json"
    EMR_CONFIGURE_APPS_URL = "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html"
    JAR_DEST = SPARK_PATH + "/jars"
    OPTIONAL_JARS = {"jets3t-0.9.0.jar": HADOOP_PATH + "/lib"}

    def __init__(self, resource_config: Dict[str, Any] = default_resource_config):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("smspark-submit")
        self.resource_config = resource_config
        self.waiter = Waiter()

    def bootstrap_smspark_submit(self) -> None:
        self.copy_aws_jars()
        self.copy_cluster_config()
        self.write_runtime_cluster_config()
        self.write_user_configuration()
        self.start_hadoop_daemons()
        self.wait_for_hadoop()

    def bootstrap_history_server(self) -> None:
        self.copy_aws_jars()
        self.copy_cluster_config()
        self.start_spark_standalone_primary()

    def copy_aws_jars(self) -> None:
        self.logger.info("copying aws jars")
        for f in glob.glob("/usr/share/aws/aws-java-sdk/*.jar"):
            shutil.copyfile(f, os.path.join(self.JAR_DEST, os.path.basename(f)))
        hadoop_aws_jar = self._get_hadoop_jar()
        shutil.copyfile(
            os.path.join(Bootstrapper.HADOOP_PATH, hadoop_aws_jar), os.path.join(self.JAR_DEST, hadoop_aws_jar)
        )

        self._copy_optional_jars()
        # copy hmclient (glue data catalog hive metastore client) jars to classpath:
        # https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore
        for f in glob.glob("/usr/share/aws/hmclient/lib/*.jar"):
            shutil.copyfile(f, os.path.join(self.JAR_DEST, os.path.basename(f)))

    def _get_hadoop_jar(self) -> str:
        for file_name in os.listdir(Bootstrapper.HADOOP_PATH):
            if file_name.startswith("hadoop-aws") and file_name.endswith(".jar"):
                self.logger.info(f"Found hadoop jar {file_name}")
                return file_name

        raise AlgorithmError("Error finding hadoop jar", caused_by=FileNotFoundError())

    def _copy_optional_jars(self) -> None:
        for jar, jar_path in self.OPTIONAL_JARS.items():
            if os.path.isfile(os.path.join(jar_path, jar)):
                self.logger.info(f"Copying optional jar {jar} from {jar_path} to {self.JAR_DEST}")
                shutil.copyfile(
                    os.path.join(jar_path, jar), os.path.join(self.JAR_DEST, jar),
                )
            else:
                self.logger.info(f"Optional jar {jar} in {jar_path} does not exist")

    def copy_cluster_config(self) -> None:
        self.logger.info("copying cluster config")

        def copy_config(src: str, dst: str) -> None:
            self.logger.info(f"copying {src} to {dst}")
            shutil.copyfile(src, dst)

        copy_config(
            "/opt/hadoop-config/hdfs-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/hdfs-site.xml",
        )
        copy_config(
            "/opt/hadoop-config/core-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/core-site.xml",
        )
        copy_config(
            "/opt/hadoop-config/yarn-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/yarn-site.xml",
        )
        copy_config(
            "/opt/hadoop-config/spark-defaults.conf", Bootstrapper.SPARK_PATH + "/conf/spark-defaults.conf",
        )
        copy_config("/opt/hadoop-config/spark-env.sh", Bootstrapper.SPARK_PATH + "/conf/spark-env.sh")

    def write_runtime_cluster_config(self) -> None:
        primary_host = self.resource_config["hosts"][0]
        primary_ip = socket.gethostbyname(primary_host)
        current_host = self.resource_config["current_host"]

        core_site_file_path = Bootstrapper.HADOOP_PATH + "/etc/hadoop/core-site.xml"
        yarn_site_file_path = Bootstrapper.HADOOP_PATH + "/etc/hadoop/yarn-site.xml"

        hadoop_env_file_path = Bootstrapper.HADOOP_PATH + "/etc/hadoop/hadoop-env.sh"
        yarn_env_file_path = Bootstrapper.HADOOP_PATH + "/etc/hadoop/yarn-env.sh"
        spark_conf_file_path = Bootstrapper.SPARK_PATH + "/conf/spark-defaults.conf"

        # Pass through environment variables to hadoop env
        with open(hadoop_env_file_path, "a") as hadoop_env_file:
            hadoop_env_file.write("export SPARK_MASTER_HOST=" + primary_ip + "\n")
            hadoop_env_file.write(
                "export AWS_CONTAINER_CREDENTIALS_RELATIVE_URI="
                + os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
                + "\n"
            )

        # Add YARN log directory
        with open(yarn_env_file_path, "a") as yarn_env_file:
            yarn_env_file.write("export YARN_LOG_DIR=/var/log/yarn/")

        # Configure ip address for name node
        with open(core_site_file_path, "r") as core_file:
            file_data = core_file.read()
        file_data = file_data.replace("nn_uri", primary_ip)
        with open(core_site_file_path, "w") as core_file:
            core_file.write(file_data)

        # Set special regional configs (e.g. S3 endpoint)
        self.set_regional_configs()

        # Configure hostname for resource manager and node manager
        with open(yarn_site_file_path, "r") as yarn_file:
            file_data = yarn_file.read()
        file_data = file_data.replace("rm_hostname", primary_ip)
        file_data = file_data.replace("nm_hostname", current_host)
        file_data = file_data.replace(
            "nm_webapp_address", "{}:{}".format(current_host, self.NODEMANAGER_WEBAPP_ADDR_PORT)
        )
        with open(yarn_site_file_path, "w") as yarn_file:
            yarn_file.write(file_data)
        with open(yarn_site_file_path, "w") as yarn_file:
            yarn_file.write(file_data)

        with open(spark_conf_file_path, "r") as spark_file:
            file_data = spark_file.read()
        file_data = file_data.replace("sd_host", primary_ip)
        with open(spark_conf_file_path, "w") as spark_file:
            spark_file.write(file_data)

        # Calculate and set Spark and Yarn resource allocation configs
        self.set_yarn_spark_resource_config()

        logging.info("Finished Yarn configuration files setup.")

    def start_hadoop_daemons(self) -> None:
        current_host = self.resource_config["current_host"]
        primary_host = self.resource_config["hosts"][0]

        # TODO: sync with EMR puppet scripts - ensure we are following best practices for starting hdfs/yarn daemons
        cmd_prep_namenode_dir = "rm -rf /opt/amazon/hadoop/hdfs/namenode && mkdir -p /opt/amazon/hadoop/hdfs/namenode"
        cmd_prep_datanode_dir = "rm -rf /opt/amazon/hadoop/hdfs/datanode && mkdir -p /opt/amazon/hadoop/hdfs/datanode"
        cmd_namenode_format = "hdfs namenode -format -force"
        cmd_namenode_start = "hdfs namenode"
        cmd_datanode_start = "hdfs datanode"
        cmd_resourcemanager_start = "yarn resourcemanager"
        cmd_nodemanager_start = "yarn nodemanager"

        if current_host == primary_host:
            subprocess.call(cmd_prep_namenode_dir, shell=True)
            subprocess.call(cmd_prep_datanode_dir, shell=True)
            subprocess.call(cmd_namenode_format, shell=True)
            subprocess.Popen(cmd_namenode_start, shell=True)
            subprocess.Popen(cmd_datanode_start, shell=True)
            subprocess.Popen(cmd_resourcemanager_start, shell=True)
            subprocess.Popen(cmd_nodemanager_start, shell=True)
            # TODO: wait for daemons to stabilize on primary + worker nodes
        else:
            subprocess.call(cmd_prep_datanode_dir, shell=True)
            subprocess.Popen(cmd_datanode_start, shell=True)
            subprocess.Popen(cmd_nodemanager_start, shell=True)

    def wait_for_hadoop(self) -> None:
        def cluster_is_up() -> bool:
            cluster_info_url = "http://{}:8042/node".format(self.resource_config["current_host"])
            try:
                resp = requests.get(cluster_info_url)
                return resp.ok
            except Exception:
                return False

        self.logger.info("waiting for cluster to be up")
        self.waiter.wait_for(predicate_fn=cluster_is_up, timeout=60.0, period=1.0)
        self.logger.info("cluster is up")

    def start_spark_standalone_primary(self) -> None:
        """Start only spark standalone's primary node for history server, since distributing workload to workers is not needed for history server.

        See more details at https://spark.apache.org/docs/latest/spark-standalone.html
        """
        cmd_start_primary = "/usr/lib/spark/sbin/start-master.sh"
        subprocess.Popen(cmd_start_primary, shell=True)

    def deserialize_user_configuration(
        self, configuration_dict_or_list: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Union[Sequence[Configuration], Configuration]:
        if isinstance(configuration_dict_or_list, dict):
            return self.deserialize_user_configuration_dict(configuration_dict_or_list)
        else:
            return self._deserialize_user_configuration_to_sequence(configuration_dict_or_list)

    def _deserialize_user_configuration_to_sequence(
        self, configuration_list: List[Dict[str, Any]]
    ) -> Sequence[Configuration]:
        return [self.deserialize_user_configuration_dict(conf) for conf in configuration_list]

    def deserialize_user_configuration_dict(self, configuration_dict: Dict[str, Any]) -> Configuration:
        if configuration_dict.get("Configurations"):
            configurations_inner = configuration_dict["Configurations"] if configuration_dict["Configurations"] else ()
            return Configuration(
                Classification=configuration_dict["Classification"],
                Properties=configuration_dict["Properties"],
                Configurations=self._deserialize_user_configuration_to_sequence(configurations_inner),
            )
        else:
            return Configuration(
                Classification=configuration_dict["Classification"], Properties=configuration_dict["Properties"],
            )

    def write_user_configuration(self) -> None:
        path = pathlib.Path(Bootstrapper.PROCESSING_CONF_INPUT_PATH)

        def _write_conf(conf: Configuration) -> None:
            logging.info("Writing user config to {}".format(conf.path))
            conf_string = conf.write_config()
            logging.info("Configuration at {} is: \n{}".format(conf.path, conf_string))

        if path.exists():
            logging.info("reading user configuration from {}".format(str(path)))
            with open(str(path), "r") as config:
                user_configuration_list_or_dict = json.load(config)
                logging.info(
                    "User configuration list or dict: {} , type {}".format(
                        user_configuration_list_or_dict, type(user_configuration_list_or_dict),
                    )
                )
                user_confs = self.deserialize_user_configuration(user_configuration_list_or_dict)
                if isinstance(user_confs, Configuration):
                    _write_conf(user_confs)
                elif isinstance(user_confs, list):
                    for user_conf in user_confs:
                        _write_conf(user_conf)
                else:
                    raise ValueError(
                        "Could not determine type of user configuration {}. Please consult {} for more information.".format(
                            user_configuration_list_or_dict, Bootstrapper.EMR_CONFIGURE_APPS_URL
                        )
                    )
        else:
            logging.info("No file at {} exists, skipping user configuration".format(str(path)))

    def set_regional_configs(self) -> None:
        regional_configs_list = self.get_regional_configs()
        for regional_config in regional_configs_list:
            logging.info("Writing regional config to {}".format(regional_config.path))
            regional_config_string = regional_config.write_config()
            logging.info("Configuration at {} is: \n{}".format(regional_config.path, regional_config_string))

    def get_regional_configs(self) -> List[Configuration]:
        aws_region = os.getenv("AWS_REGION")
        if aws_region is None:
            logging.warning("Unable to detect AWS region from environment variable AWS_REGION")
            return []
        elif aws_region in ["cn-northwest-1", "cn-north-1"]:
            aws_domain = "amazonaws.com.cn"
            s3_endpoint = f"s3.{aws_region}.{aws_domain}"
        elif aws_region in ["us-gov-west-1", "us-gov-east-1"]:
            aws_domain = "amazonaws.com"
            s3_endpoint = f"s3.{aws_region}.{aws_domain}"
        else:
            # no special regional configs needed
            return []

        return [Configuration(Classification="core-site", Properties={"fs.s3a.endpoint": s3_endpoint})]

    def load_processing_job_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.PROCESSING_JOB_CONFIG_PATH):
            logging.warning(f"Path does not exist: {self.PROCESSING_JOB_CONFIG_PATH}")
            return {}
        with open(self.PROCESSING_JOB_CONFIG_PATH, "r") as f:
            return json.loads(f.read())

    def load_instance_type_info(self) -> Dict[str, Any]:
        if not os.path.exists(self.INSTANCE_TYPE_INFO_PATH):
            logging.warning(f"Path does not exist: {self.INSTANCE_TYPE_INFO_PATH}")
            return {}
        with open(self.INSTANCE_TYPE_INFO_PATH, "r") as f:
            instance_type_info_list = json.loads(f.read())
            return {instance["InstanceType"]: instance for instance in instance_type_info_list}

    def set_yarn_spark_resource_config(self) -> None:
        processing_job_config = self.load_processing_job_config()
        instance_type_info = self.load_instance_type_info()

        if processing_job_config and instance_type_info:
            instance_type = processing_job_config["ProcessingResources"]["ClusterConfig"]["InstanceType"].replace(
                "ml.", ""
            )
            instance_count = processing_job_config["ProcessingResources"]["ClusterConfig"]["InstanceCount"]
            instance_type_info = instance_type_info[instance_type]
            instance_mem_mb = instance_type_info["MemoryInfo"]["SizeInMiB"]
            instance_cores = instance_type_info["VCpuInfo"]["DefaultVCpus"]
            logging.info(
                f"Detected instance type: {instance_type} with "
                f"total memory: {instance_mem_mb}M and total cores: {instance_cores}"
            )
        else:
            instance_count = 1
            instance_mem_mb = int(psutil.virtual_memory().total / (1024 * 1024))
            instance_cores = psutil.cpu_count(logical=True)
            logging.warning(
                f"Failed to detect instance type config. "
                f"Found total memory: {instance_mem_mb}M and total cores: {instance_cores}"
            )

        yarn_config, spark_config = self.get_yarn_spark_resource_config(instance_count, instance_mem_mb, instance_cores)

        logging.info("Writing default config to {}".format(yarn_config.path))
        yarn_config_string = yarn_config.write_config()
        logging.info("Configuration at {} is: \n{}".format(yarn_config.path, yarn_config_string))

        logging.info("Writing default config to {}".format(spark_config.path))
        spark_config_string = spark_config.write_config()
        logging.info("Configuration at {} is: \n{}".format(spark_config.path, spark_config_string))

    def get_yarn_spark_resource_config(
        self, instance_count: int, instance_mem_mb: int, instance_cores: int
    ) -> Tuple[Configuration, Configuration]:
        executor_cores = instance_cores
        executor_count_per_instance = int(instance_cores / executor_cores)
        executor_count_total = instance_count * executor_count_per_instance
        default_parallelism = instance_count * instance_cores * 2

        # Let's leave 3% of the instance memory free
        instance_mem_mb = int(instance_mem_mb * 0.97)

        driver_mem_mb = 2 * 1024
        driver_mem_ovr_pct = 0.1
        driver_mem_ovr_mb = int(driver_mem_mb * driver_mem_ovr_pct)
        executor_mem_ovr_pct = 0.1
        executor_mem_mb = int(
            (instance_mem_mb - driver_mem_mb - driver_mem_ovr_mb)
            / (executor_count_per_instance + executor_count_per_instance * executor_mem_ovr_pct)
        )
        executor_mem_ovr_mb = int(executor_mem_mb * executor_mem_ovr_pct)

        driver_gc_config = (
            "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 "
            "-XX:+CMSClassUnloadingEnabled"
        )
        driver_java_opts = f"-XX:OnOutOfMemoryError='kill -9 %p' " f"{driver_gc_config}"

        executor_gc_config = (
            f"-XX:+UseParallelGC -XX:InitiatingHeapOccupancyPercent=70 "
            f"-XX:ConcGCThreads={max(int(executor_cores / 4), 1)} "
            f"-XX:ParallelGCThreads={max(int(3 * executor_cores / 4), 1)} "
        )
        executor_java_opts = (
            f"-verbose:gc -XX:OnOutOfMemoryError='kill -9 %p' "
            f"-XX:+PrintGCDetails -XX:+PrintGCDateStamps "
            f"{executor_gc_config}"
        )

        yarn_site_config = Configuration(
            "yarn-site",
            {
                "yarn.scheduler.minimum-allocation-mb": "1",
                "yarn.scheduler.maximum-allocation-mb": str(instance_mem_mb),
                "yarn.scheduler.minimum-allocation-vcores": "1",
                "yarn.scheduler.maximum-allocation-vcores": str(instance_cores),
                "yarn.nodemanager.resource.memory-mb": str(instance_mem_mb),
                "yarn.nodemanager.resource.cpu-vcores": str(instance_cores),
            },
        )

        spark_defaults_config = Configuration(
            "spark-defaults",
            {
                "spark.driver.memory": f"{driver_mem_mb}m",
                "spark.driver.memoryOverhead": f"{driver_mem_ovr_mb}m",
                "spark.driver.defaultJavaOptions": f"{driver_java_opts}",
                "spark.executor.memory": f"{executor_mem_mb}m",
                "spark.executor.memoryOverhead": f"{executor_mem_ovr_mb}m",
                "spark.executor.cores": f"{executor_cores}",
                "spark.executor.defaultJavaOptions": f"{executor_java_opts}",
                "spark.executor.instances": f"{executor_count_total}",
                "spark.default.parallelism": f"{default_parallelism}",
            },
        )

        return yarn_site_config, spark_defaults_config

# flake8: noqa
import glob
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
from typing import Any, Dict, List, Union

import psutil
import requests
from smspark.config import Configuration
from smspark.defaults import default_resource_config
from smspark.waiter import Waiter


class Bootstrapper:
    """Initializes the cluster."""

    NODEMANAGER_WEBAPP_ADDR_PORT = 8042

    HADOOP_CONFIG_PATH = "/opt/hadoop-config/"
    HADOOP_PATH = "/usr/lib/hadoop"
    SPARK_PATH = "/usr/lib/spark"
    HIVE_PATH = "/usr/lib/hive"
    PROCESSING_CONF_INPUT_PATH = "/opt/ml/processing/input/conf/configuration.json"
    EMR_CONFIGURE_APPS_URL = "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html"

    def __init__(self, resource_config: Dict[str, Any] = default_resource_config):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("smspark-submit")
        self.resource_config = resource_config
        self.waiter = Waiter()

    def bootstrap_smspark_submit(self):
        self.copy_aws_jars()
        self.copy_cluster_config()
        self.write_runtime_cluster_config()
        self.write_user_configuration()
        self.start_hadoop_daemons()
        self.wait_for_hadoop()

    def bootstrap_history_server(self):
        self.copy_aws_jars()
        self.copy_cluster_config()
        self.start_spark_standalone_primary()

    def copy_aws_jars(self) -> None:
        self.logger.info("copying aws jars")
        jar_dest = Bootstrapper.SPARK_PATH + "/jars"
        for f in glob.glob("/usr/share/aws/aws-java-sdk/*.jar"):
            shutil.copyfile(f, os.path.join(jar_dest, os.path.basename(f)))
        hadoop_aws_jar = "hadoop-aws-2.8.5-amzn-5.jar"
        jets3t_jar = "jets3t-0.9.0.jar"
        shutil.copyfile(
            os.path.join(Bootstrapper.HADOOP_PATH, hadoop_aws_jar), os.path.join(jar_dest, hadoop_aws_jar),
        )
        # this jar required for using s3a client
        shutil.copyfile(
            os.path.join(Bootstrapper.HADOOP_PATH + "/lib", jets3t_jar), os.path.join(jar_dest, jets3t_jar),
        )
        # copy hmclient (glue data catalog hive metastore client) jars to classpath:
        # https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore
        for f in glob.glob("/usr/share/aws/hmclient/lib/*.jar"):
            shutil.copyfile(f, os.path.join(jar_dest, os.path.basename(f)))

    def copy_cluster_config(self) -> None:
        self.logger.info("copying cluster config")

        def copy_config(src, dst):
            self.logger.info("copying {} to {}".format(src, dst))
            shutil.copyfile(src, dst)

        copy_config("/opt/hadoop-config/hdfs-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/hdfs-site.xml")
        copy_config("/opt/hadoop-config/core-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/core-site.xml")
        copy_config("/opt/hadoop-config/yarn-site.xml", Bootstrapper.HADOOP_PATH + "/etc/hadoop/yarn-site.xml")
        copy_config("/opt/hadoop-config/spark-defaults.conf", Bootstrapper.SPARK_PATH + "/conf/spark-defaults.conf")
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

        # Configure hostname for resource manager and node manager
        with open(yarn_site_file_path, "r") as yarn_file:
            file_data = yarn_file.read()
        file_data = file_data.replace("rm_hostname", primary_ip)
        file_data = file_data.replace("nm_hostname", current_host)
        file_data = file_data.replace("nm_webapp_address", "{}:{}".format(current_host, 8042))
        file_data = file_data.replace(
            "nm_webapp_address", "{}:{}".format(current_host, self.NODEMANAGER_WEBAPP_ADDR_PORT)
        )
        with open(yarn_site_file_path, "w") as yarn_file:
            yarn_file.write(file_data)

        # Configure yarn resource limitation
        mem = int(psutil.virtual_memory().total / (1024 * 1024))  # total physical memory in mb
        cores = psutil.cpu_count(logical=True)  # vCPUs

        minimum_allocation_mb = "1"
        maximum_allocation_mb = str(mem)
        minimum_allocation_vcores = "1"
        maximum_allocation_vcores = str(cores)
        # Add some residual in memory due to rounding in memory allocation
        memory_mb_total = str(mem + 2048)
        # Ensure core allocations
        cpu_vcores_total = str(cores * 16)

        with open(yarn_site_file_path, "r") as yarn_file:
            file_data = yarn_file.read()
        file_data = file_data.replace("minimum_allocation_mb", minimum_allocation_mb)
        file_data = file_data.replace("maximum_allocation_mb", maximum_allocation_mb)
        file_data = file_data.replace("minimum_allocation_vcores", minimum_allocation_vcores)
        file_data = file_data.replace("maximum_allocation_vcores", maximum_allocation_vcores)
        file_data = file_data.replace("memory_mb_total", memory_mb_total)
        file_data = file_data.replace("cpu_vcores_total", cpu_vcores_total)
        with open(yarn_site_file_path, "w") as yarn_file:
            yarn_file.write(file_data)

        # Configure Spark defaults
        with open(spark_conf_file_path, "r") as spark_file:
            file_data = spark_file.read()
        file_data = file_data.replace("sd_host", primary_ip)
        file_data = file_data.replace("exec_mem", str(int((mem / 3) * 2.2)) + "m")
        file_data = file_data.replace("exec_cores", str(min(5, cores - 1)))
        with open(spark_conf_file_path, "w") as spark_file:
            spark_file.write(file_data)
        print("Finished Yarn configuration files setup.\n")

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

        self.waiter.wait_for(predicate_fn=cluster_is_up, timeout=60.0, period=1.0)

    def start_spark_standalone_primary(self) -> None:
        """Start only spark standalone's primary node for history server, since distributing workload to workers is not needed for history server.

        See more details at https://spark.apache.org/docs/latest/spark-standalone.html
        """
        cmd_start_primary = "/usr/lib/spark/sbin/start-master.sh"
        subprocess.Popen(cmd_start_primary, shell=True)

    def deserialize_user_configuration(self, configuration_dict_or_list) -> Union[List[Configuration], Configuration]:
        if isinstance(configuration_dict_or_list, dict):
            return self.deserialize_user_configuration_dict(configuration_dict_or_list)
        elif isinstance(configuration_dict_or_list, list):
            list_of_configurations = []
            for conf in configuration_dict_or_list:
                configuration = self.deserialize_user_configuration_dict(conf)
                list_of_configurations.append(configuration)
        return list_of_configurations

    def deserialize_user_configuration_dict(self, configuration_dict: dict) -> Configuration:
        if configuration_dict.get("Configurations"):
            configurations_inner = configuration_dict["Configurations"] if configuration_dict["Configurations"] else ()
            return Configuration(
                Classification=configuration_dict["Classification"],
                Properties=configuration_dict["Properties"],
                Configurations=self.deserialize_user_configuration(configurations_inner),
            )
        else:
            return Configuration(
                Classification=configuration_dict["Classification"], Properties=configuration_dict["Properties"]
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

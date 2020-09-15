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
from smspark.config import Configuration


def test_core_site_xml() -> None:
    configuration = Configuration("core-site", {"hadoop.security.groups.cache.secs": "250"})

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == "  <property>\n    <name>hadoop.security.groups.cache.secs</name>\n    <value>250</value>\n  </property>\n"
    )


def test_hadoop_env_sh() -> None:
    configuration = Configuration(
        "hadoop-env",
        {},
        Configurations=[
            Configuration("export", {"HADOOP_DATANODE_HEAPSIZE": "2048", "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"},)
        ],
    )

    serialized_conf = configuration.serialized

    assert serialized_conf == "export HADOOP_DATANODE_HEAPSIZE=2048\nexport HADOOP_NAMENODE_OPTS=-XX:GCTimeRatio=19\n"


def test_hadoop_log4j() -> None:
    configuration = Configuration(
        "hadoop-log4j",
        {"hadoop.root.logger": "INFO,console", "hadoop.log.dir": "/var/log/hadoop", "hadoop.log.file": "hadoop.log",},
    )

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == "hadoop.root.logger=INFO,console\nhadoop.log.dir=/var/log/hadoop\nhadoop.log.file=hadoop.log\n"
    )


def test_hive_env() -> None:
    configuration = Configuration(
        "hive-env",
        {},
        Configurations=[Configuration("export", {"HADOOP_HEAPSIZE": "1000", "USE_HADOOP_SLF4J_BINDING": "false"})],
    )

    serialized_conf = configuration.serialized

    assert serialized_conf == "export HADOOP_HEAPSIZE=1000\nexport USE_HADOOP_SLF4J_BINDING=false\n"


def test_hive_log4j() -> None:
    configuration = Configuration("hive-log4j", {"property.hive.log.level": "INFO"})

    serialized_conf = configuration.serialized

    assert serialized_conf == "property.hive.log.level=INFO\n"


def test_hive_exec_log4j() -> None:
    configuration = Configuration(
        "hive-exec-log4j", {"loggers": "NIOServerCnxn,ClientCnxnSocketNIO,DataNucleus,Datastore,JPOX"},
    )

    serialized_conf = configuration.serialized

    assert serialized_conf == "loggers=NIOServerCnxn,ClientCnxnSocketNIO,DataNucleus,Datastore,JPOX\n"


def test_hive_site() -> None:
    configuration = Configuration(
        "hive-site",
        {
            "hive.execution.engine": "tez",
            "hive.security.metastore.authorization.manager": "org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider",
        },
    )

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == "  <property>\n    <name>hive.execution.engine</name>\n    <value>tez</value>\n  </property>\n  <property>\n    <name>hive.security.metastore.authorization.manager</name>\n    <value>org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider</value>\n  </property>\n"
    )


def test_spark_defaults_conf():
    configuration = Configuration("spark-defaults", {"spark.master": "yarn", "spark.executor.memory": "4096M"})

    serialized_conf = configuration.serialized

    assert serialized_conf == "spark.master yarn\nspark.executor.memory 4096M\n"


def test_spark_env():
    configuration = Configuration(
        "hadoop-env",
        {},
        Configurations=[
            Configuration("export", {"SPARK_MASTER_PORT": "7077", "SPARK_MASTER_IP": "$STANDALONE_SPARK_MASTER_HOST"},)
        ],
    )

    serialized_conf = configuration.serialized

    assert serialized_conf == "export SPARK_MASTER_PORT=7077\nexport SPARK_MASTER_IP=$STANDALONE_SPARK_MASTER_HOST\n"


def test_spark_log4j_properties():
    configuration = Configuration(
        "spark-log4j", {"spark.yarn.app.container.log.dir": "/var/log/spark/user/${user.name}"}
    )

    serialized_conf = configuration.serialized

    assert serialized_conf == "spark.yarn.app.container.log.dir=/var/log/spark/user/${user.name}\n"


def test_spark_hive_site():
    configuration = Configuration("spark-hive-site", {"javax.jdo.option.ConnectionUserName": "hive"},)

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == "  <property>\n    <name>javax.jdo.option.ConnectionUserName</name>\n    <value>hive</value>\n  </property>\n"
    )


def test_spark_metrics_properties():
    configuration = Configuration("spark-metrics", {"*.sink.statsd.prefix": "spark"})

    serialized_conf = configuration.serialized

    assert serialized_conf == "*.sink.statsd.prefix=spark\n"


def test_yarn_env():
    configuration = Configuration(
        "yarn-env",
        {},
        Configurations=[
            Configuration(
                "export",
                {
                    "YARN_OPTS": "\"$YARN_OPTS -XX:OnOutOfMemoryError='kill -9 %p'\"",
                    "YARN_PROXYSERVER_HEAPSIZE": "2416",
                    "YARN_NODEMANAGER_HEAPSIZE": "2048",
                    "YARN_RESOURCEMANAGER_HEAPSIZE": "2416",
                },
            )
        ],
    )

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == """export YARN_OPTS="$YARN_OPTS -XX:OnOutOfMemoryError='kill -9 %p'"\nexport YARN_PROXYSERVER_HEAPSIZE=2416\nexport YARN_NODEMANAGER_HEAPSIZE=2048\nexport YARN_RESOURCEMANAGER_HEAPSIZE=2416\n"""
    )


def test_yarn_size():
    configuration = Configuration("yarn-site", {"yarn.log-aggregation-enable": "true"},)

    serialized_conf = configuration.serialized

    assert (
        serialized_conf
        == "  <property>\n    <name>yarn.log-aggregation-enable</name>\n    <value>true</value>\n  </property>\n"
    )

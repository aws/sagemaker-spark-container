#!/bin/bash

# Script to install Hadoop ecosystem packages from EMR release 5.29.0
# TODO: SM-PROCESSING-516 - trim down the number of packages from the EMR release

#set -e

source scripts/shared.sh

parse_build_context "$@"

packages=(
# Bigtop
'bigtop-groovy-2.4.4-1.amzn1.noarch.rpm'
'bigtop-jsvc-1.0.15-1.amzn1.x86_64.rpm'
'bigtop-jsvc-debuginfo-1.0.15-1.amzn1.x86_64.rpm'
'bigtop-tomcat-6.0.48-1.amzn1.noarch.rpm'
'bigtop-utils-1.2.0-1.amzn1.noarch.rpm'

# EMR Goodies
'aws-hm-client-1.11.0-1.amzn1.noarch.rpm'
'aws-java-sdk-1.11.682-1.amzn1.noarch.rpm'
'cloudwatch-sink-1.2.0-1.amzn1.noarch.rpm'
'emr-ddb-4.13.0-1.amzn1.noarch.rpm'
'emr-ddb-hadoop-4.13.0-1.amzn1.noarch.rpm'
'emr-ddb-hive-4.13.0-1.amzn1.noarch.rpm'
'emr-docker-apps-1.6.0-1.noarch.rpm'
'emr-goodies-2.12.0-1.amzn1.noarch.rpm'
'emr-goodies-hadoop-2.12.0-1.amzn1.noarch.rpm'
'emr-goodies-hive-2.12.0-1.amzn1.noarch.rpm'
'emr-goodies-parquet-2.12.0-1.amzn1.noarch.rpm'
'emr-goodies-spark-2.12.0-1.amzn1.noarch.rpm'
'emr-kinesis-3.4.0-1.amzn1.noarch.rpm'
'emr-kinesis-cascading-3.4.0-1.amzn1.noarch.rpm'
'emr-kinesis-hadoop-3.4.0-1.amzn1.noarch.rpm'
'emr-kinesis-hive-3.4.0-1.amzn1.noarch.rpm'
'emr-kinesis-pig-3.4.0-1.amzn1.noarch.rpm'
'emr-kinesis-samples-3.4.0-1.amzn1.noarch.rpm'
'emr-record-server-1.4.0-1.amzn1.noarch.rpm'
'emr-s3-select-1.4.0-1.amzn1.noarch.rpm'
'emr-samples-1.0.1-1.amzn1.noarch.rpm'
'emr-scripts-2.3.0-1.amzn1.noarch.rpm'
'emrfs-2.38.0-1.amzn1.noarch.rpm'

# Hadoop 2.8.5
'hadoop-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-client-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-conf-pseudo-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-debuginfo-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-doc-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-datanode-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-fuse-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-journalnode-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-namenode-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-secondarynamenode-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-hdfs-zkfc-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-kms-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-libhdfs-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-libhdfs-devel-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-lzo-0.4.19-1.amzn1.x86_64.rpm'
'hadoop-lzo-debuginfo-0.4.19-1.amzn1.x86_64.rpm'
'hadoop-mapreduce-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-mapreduce-historyserver-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-yarn-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-yarn-nodemanager-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-yarn-proxyserver-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-yarn-resourcemanager-2.8.5.amzn.5-1.amzn1.x86_64.rpm'
'hadoop-yarn-timelineserver-2.8.5.amzn.5-1.amzn1.x86_64.rpm'

# Spark 2.4.4
'spark-core-2.4.4-1.amzn1.noarch.rpm'
'spark-datanucleus-2.4.4-1.amzn1.noarch.rpm'
'spark-external-2.4.4-1.amzn1.noarch.rpm'
'spark-history-server-2.4.4-1.amzn1.noarch.rpm'
'spark-master-2.4.4-1.amzn1.noarch.rpm'
'spark-python-2.4.4-1.amzn1.noarch.rpm'
'spark-thriftserver-2.4.4-1.amzn1.noarch.rpm'
'spark-worker-2.4.4-1.amzn1.noarch.rpm'
'spark-yarn-shuffle-2.4.4-1.amzn1.noarch.rpm'

# Hive 2.3.6
'hive-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-hbase-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-hcatalog-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-hcatalog-server-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-jdbc-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-metastore-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-server2-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-webhcat-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hive-webhcat-server-2.3.6.amzn.1-1.amzn1.noarch.rpm'
'hbase-1.4.10-1.amzn1.noarch.rpm'
'hbase-doc-1.4.10-1.amzn1.noarch.rpm'
'hbase-master-1.4.10-1.amzn1.noarch.rpm'
'hbase-regionserver-1.4.10-1.amzn1.noarch.rpm'
'hbase-rest-1.4.10-1.amzn1.noarch.rpm'
'hbase-thrift-1.4.10-1.amzn1.noarch.rpm'
'hbase-thrift2-1.4.10-1.amzn1.noarch.rpm'

# Zookeeper
'zookeeper-3.4.14-1.amzn1.x86_64.rpm'
'zookeeper-debuginfo-3.4.14-1.amzn1.x86_64.rpm'
'zookeeper-native-3.4.14-1.amzn1.x86_64.rpm'
'zookeeper-rest-3.4.14-1.amzn1.x86_64.rpm'
'zookeeper-server-3.4.14-1.amzn1.x86_64.rpm'
)

# change directory to scripts folder
cd ./scripts
# Create a tar archive containing RPMs, so we don't need AWS credentials during the docker build.
# Docker automatically untars archives in its build context.
archive_dir="emr-spark-packages"
archive_name="emr-spark-packages.tar"
mkdir $archive_dir || true

i=0
packageList=""
while [ "x${packages[i]}" != "x" ]
do
   aws s3 cp "s3://repo.us-east-1.emr.amazonaws.com/apps-repository/emr-5.29.0/8a852a8e-d51c-47a8-a47e-e3df88018990/${packages[i]}" $archive_dir
   packageList="${packageList} /usr/${packages[i]}"
   i=$(( $i + 1 ))
done

tar -zcvf $archive_name $archive_dir
mv $archive_name ../$build_context

rm -rf $archive_dir

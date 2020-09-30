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
import subprocess

import pytest


@pytest.fixture
def input_data() -> str:
    return "file:///opt/ml/processing/input/data/data.jsonl"


@pytest.fixture
def output_data() -> str:
    return "file:///opt/ml/processing/output/data"


@pytest.fixture
def verbose_opt() -> str:
    return "--verbose"


def test_pyspark_multinode(input_data: str, output_data: str, verbose_opt: str) -> None:
    input = "--input {}".format(input_data)
    output = "--output {}".format(output_data)
    py_files_arg = "--py-files /opt/ml/processing/input/code/python/hello_py_spark/hello_py_spark_udfs.py"
    app_py = "/opt/ml/processing/input/code/python/hello_py_spark/hello_py_spark_app.py"
    docker_compose_cmd = (
        f"CMD='{py_files_arg} {verbose_opt} {app_py} {input} {output}' docker-compose up --force-recreate"
    )

    print(docker_compose_cmd)
    docker_compose_proc = subprocess.run(
        docker_compose_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, encoding="utf-8",
    )

    stdout = docker_compose_proc.stdout
    print(stdout)

    # assert exit code of docker compose command.
    assert 0 == docker_compose_proc.returncode

    # assert the expected Python script executed
    assert "Hello World, this is PySpark" in stdout

    # Application code does "df.show()"
    assert "only showing top 20 rows" in stdout

    # from spark-defaults configuration in configuration.json
    assert "(spark.executor.memory,2g)" in stdout
    assert "(spark.executor.cores,1)" in stdout

    # assert on stdout from docker compose to get exit code of each container.
    assert 2 == stdout.count("exited with code 0")

    # TODO: assert output contents

    try:
        print("\nRunning docker-compose down ...")
        subprocess.run(["docker-compose", "down"])
    except subprocess.CalledProcessError:
        print("docker-compose down command failed !!!")


def test_scala_spark_multinode(input_data: str, output_data: str, verbose_opt: str) -> None:
    input = "--input {}".format(input_data)
    output = "--output {}".format(output_data)
    host_jars_dir = "./test/resources/code/scala/hello-scala-spark/lib_managed/jars/org.json4s/json4s-native_2.12"
    container_jars_dir = "/opt/ml/processing/input/jars"
    jars_mount = f"{host_jars_dir}:{container_jars_dir}"
    jars_arg = f"--jars {container_jars_dir}"
    class_arg = "--class com.amazonaws.sagemaker.spark.test.HelloScalaSparkApp"
    app_jar = "/opt/ml/processing/input/code/scala/hello-scala-spark/target/scala-2.12/hello-scala-spark_2.12-1.0.jar"
    docker_compose_cmd = (
        f"JARS_MOUNT={jars_mount} "
        f"CMD='{jars_arg} {class_arg} {verbose_opt} {app_jar} {input} {output}' "
        "docker-compose up --force-recreate"  # noqa
    )

    print(docker_compose_cmd)

    docker_compose_proc = subprocess.run(
        docker_compose_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, encoding="utf-8",
    )

    stdout = docker_compose_proc.stdout
    print(stdout)

    # assert exit code of docker compose command.
    assert 0 == docker_compose_proc.returncode

    # assert the expected Scala main class executed
    assert "Hello World, this is Scala-Spark!" in stdout

    # Application code does "df.show()"
    assert "only showing top 20 rows" in stdout

    # from spark-defaults configuration in configuration.json
    assert "(spark.executor.memory,2g)" in stdout
    assert "(spark.executor.cores,1)" in stdout

    # assert on stdout from docker compose to get exit code of each container.
    assert 2 == stdout.count("exited with code 0")

    # assert executor logs included in container logs
    assert "I'm an executor" in stdout

    # TODO: assert output contents

    try:
        print("\nRunning docker-compose down ...")
        subprocess.run(["docker-compose", "down"])
    except subprocess.CalledProcessError:
        print("docker-compose down command failed !!!")


def test_java_spark_multinode(input_data: str, output_data: str, verbose_opt: str) -> None:
    input = "--input {}".format(input_data)
    output = "--output {}".format(output_data)
    class_arg = "--class com.amazonaws.sagemaker.spark.test.HelloJavaSparkApp"
    app_jar = "/opt/ml/processing/input/code/java/hello-java-spark/target/hello-java-spark-1.0-SNAPSHOT.jar"
    docker_compose_cmd = (
        f"CMD='{class_arg} {verbose_opt} {app_jar} {input} {output}' docker-compose up --force-recreate"
    )

    print(docker_compose_cmd)
    docker_compose_proc = subprocess.run(
        docker_compose_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, encoding="utf-8",
    )

    stdout = docker_compose_proc.stdout
    print(stdout)

    # assert exit code of docker compose command.
    assert 0 == docker_compose_proc.returncode

    # assert the expected Java main class executed
    assert "Hello World, this is Java-Spark!" in stdout

    # Application code does "df.show()"
    assert "only showing top 20 rows" in stdout

    # from spark-defaults configuration in configuration.json
    assert "(spark.executor.memory,2g)" in stdout
    assert "(spark.executor.cores,1)" in stdout

    # assert on stdout from docker compose to get exit code of each container.
    assert 2 == stdout.count("exited with code 0")

    # TODO: assert output contents

    try:
        print("\nRunning docker-compose down ...")
        subprocess.run(["docker-compose", "down"])
    except subprocess.CalledProcessError:
        print("docker-compose down command failed !!!")

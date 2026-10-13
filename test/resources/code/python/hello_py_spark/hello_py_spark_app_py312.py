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
import argparse
import subprocess
import sys
import time

# Import local module to test spark-submit--py-files dependencies
import hello_py_spark_udfs as udfs
from pyspark.ml import Pipeline  # importing to test mllib DLLs like liblapack.so
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import numpy
import boto3

if __name__ == "__main__":
    print("Hello World, this is PySpark!")

    # Validate that the default python3 is 3.12
    major, minor = sys.version_info.major, sys.version_info.minor
    print(f"Python version: {major}.{minor}")
    assert major == 3 and minor == 12, (
        f"Expected Python 3.12 but got {major}.{minor}. "
        "The container PATH may be resolving to the wrong python3 binary."
    )

    # Python 3.9+ dict merge operator
    d1 = {"k1": "k1 from d1", "k2": "k2 from d1"}
    d2 = {"k2": "k2 from d2", "k3": "k3 from d2"}
    print(d1 | d2)

    # Python 3.12 f-string improvements — nested quotes are allowed
    name = "PySpark"
    print(f"Running {"PySpark 3.12 test" if name else "unknown"}")

    print(f"numpy version {numpy.__version__}")
    print(f"boto3 version {boto3.__version__}")

    parser = argparse.ArgumentParser(description="inputs and outputs")
    parser.add_argument("--input", type=str, help="path to input data")
    parser.add_argument("--output", required=False, type=str, help="path to output data")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("SparkContainerTestApp").getOrCreate()
    print("Created spark context")

    # Validate executors also run Python 3.12: both the PySpark worker and a bare
    # `python3` subprocess. YARN does not whitelist PATH for containers, so the
    # subprocess check covers python3 resolution outside the image's ENV PATH.
    def executor_python_versions(_):
        cmd = ["python3", "-c", "import sys; print('%d.%d' % sys.version_info[:2])"]
        sub = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
        return [(f"{sys.version_info.major}.{sys.version_info.minor}", sub)]

    rdd = spark.sparkContext.parallelize(range(4), 4)
    executor_versions = set(rdd.mapPartitions(executor_python_versions).collect())
    print(f"Executor python versions (worker, python3 subprocess): {executor_versions}")
    assert executor_versions == {("3.12", "3.12")}, (
        f"Expected executors to use Python 3.12 but got {executor_versions}. "
        "The executor environment may be resolving to the wrong python3 binary."
    )

    sqlContext = SQLContext(spark.sparkContext)
    print("Created sql context")

    # Load test data set
    inputPath = args.input
    salesDF = spark.read.json(inputPath)
    print("Loaded data")
    salesDF.printSchema()

    salesDF.createOrReplaceTempView("sales")
    topDF = spark.sql("SELECT date, sale FROM sales WHERE sale > 750")
    # Show the first 20 rows of the dataframe
    topDF.show()
    time.sleep(60)

    # Calculate average sales by date
    averageSalesPerDay = salesDF.groupBy("date").avg().collect()
    print("Calculated data")

    outputPath = args.output

    # Define a UDF that doubles an integer column
    # The UDF function is imported from local module to test spark-submit--py-files dependencies
    double_udf_int = udf(udfs.double_x, IntegerType())

    # Save transformed data set to disk
    salesDF.select("date", "sale", double_udf_int("sale").alias("sale_double")).write.json(outputPath)
    print("Saved data")

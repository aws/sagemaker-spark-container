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
import time

# Import local module to test spark-submit--py-files dependencies
import hello_py_spark_udfs as udfs
from pyspark.ml import Pipeline  # importing to test mllib DLLs like liblapack.so
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import numpy
import sagemaker
import boto3

if __name__ == "__main__":
    print("Hello World, this is PySpark!")
    d1 = {"k1": "k1 from d1", "k2": "k2 from d1"}
    d2 = {"k2": "k2 from d2", "k3": "k3 from d2"}
    print(d1 | d2)
    print(f"numpy version {numpy.__version__}")
    print(f"sagemaker version {sagemaker.__version__}")
    print(f"boto3 version {boto3.__version__}")

    parser = argparse.ArgumentParser(description="inputs and outputs")
    parser.add_argument("--input", type=str, help="path to input data")
    parser.add_argument("--output", required=False, type=str, help="path to output data")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("SparkContainerTestApp").getOrCreate()
    print("Created spark context")
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

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
from datetime import datetime

from sagemaker.s3 import S3Downloader, S3Uploader
from sagemaker.spark.processing import PySparkProcessor


def test_sagemaker_spark_processor_default_tag(spark_version, role, sagemaker_session, sagemaker_client, instance_type):
    """Test that spark processor works with default tag"""
    spark = PySparkProcessor(
        base_job_name="sm-spark-py",
        framework_version=spark_version,
        role=role,
        instance_count=1,
        instance_type=instance_type,
        max_runtime_in_seconds=1200,
        sagemaker_session=sagemaker_session,
    )
    bucket = spark.sagemaker_session.default_bucket()
    timestamp = datetime.now().isoformat()
    output_data_uri = "s3://{}/spark/output/sales/{}".format(bucket, timestamp)
    spark_event_logs_key_prefix = "spark/spark-events/{}".format(timestamp)
    spark_event_logs_s3_uri = "s3://{}/{}".format(bucket, spark_event_logs_key_prefix)

    with open("test/resources/data/files/data.jsonl") as data:
        body = data.read()
        input_data_uri = "s3://{}/spark/input/data.jsonl".format(bucket)
        S3Uploader.upload_string_as_file_body(
            body=body, desired_s3_uri=input_data_uri, sagemaker_session=sagemaker_session
        )

    spark.run(
        submit_app="test/resources/code/python/hello_py_spark/hello_py_spark_app.py",
        submit_py_files=["test/resources/code/python/hello_py_spark/hello_py_spark_udfs.py"],
        arguments=["--input", input_data_uri, "--output", output_data_uri],
        spark_event_logs_s3_uri=spark_event_logs_s3_uri,
        wait=True,
    )

    processing_job = spark.latest_job
    waiter = sagemaker_client.get_waiter("processing_job_completed_or_stopped")
    waiter.wait(
        ProcessingJobName=processing_job.job_name,
        # poll every 15 seconds. timeout after 15 minutes.
        WaiterConfig={"Delay": 15, "MaxAttempts": 60},
    )

    output_contents = S3Downloader.list(output_data_uri, sagemaker_session=sagemaker_session)
    assert len(output_contents) != 0

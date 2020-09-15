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
from sagemaker.spark.processing import PySparkProcessor


def test_spark_app_error(tag, role, image_uri, sagemaker_session):
    """Submits a PySpark app which is scripted to exit with error code 1"""
    spark = PySparkProcessor(
        base_job_name="sm-spark-app-error",
        framework_version=tag,
        image_uri=image_uri,
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        max_runtime_in_seconds=1200,
        sagemaker_session=sagemaker_session,
    )

    try:
        spark.run(
            submit_app="test/resources/code/python/py_spark_app_error/py_spark_app_error.py", wait=True, logs=False,
        )
    except Exception:
        pass  # this job is expected to fail
    processing_job = spark.latest_job

    describe_response = processing_job.describe()
    assert "AlgorithmError: See job logs for more information" == describe_response["FailureReason"]
    assert "Algorithm Error: (caused by CalledProcessError)" in describe_response["ExitMessage"]
    assert "returned non-zero exit status 1" in describe_response["ExitMessage"]

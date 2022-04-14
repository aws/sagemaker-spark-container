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
import logging
import os
import subprocess
import time

import boto3
import requests.packages.urllib3 as urllib3
from requests.packages.urllib3.util.retry import Retry
from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor

HISTORY_SERVER_ENDPOINT = "http://0.0.0.0/proxy/15050"
SPARK_APPLICATION_URL_SUFFIX = "/history/application_1594922484246_0001/1/jobs/"


def test_history_server(tag, role, image_uri, sagemaker_session, region):
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        framework_version=tag,
        image_uri=image_uri,
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        max_runtime_in_seconds=1200,
        sagemaker_session=sagemaker_session,
    )
    bucket = sagemaker_session.default_bucket()
    spark_event_logs_key_prefix = "spark/spark-history-fs"
    spark_event_logs_s3_uri = "s3://{}/{}".format(bucket, spark_event_logs_key_prefix)
    spark_event_log_local_path = "test/resources/data/files/sample_spark_event_logs"
    file_name = "sample_spark_event_logs"
    file_size = os.path.getsize(spark_event_log_local_path)

    with open("test/resources/data/files/sample_spark_event_logs") as data:
        body = data.read()
        S3Uploader.upload_string_as_file_body(
            body=body,
            desired_s3_uri=f"{spark_event_logs_s3_uri}/{file_name}",
            sagemaker_session=sagemaker_session,
        )

    _wait_for_file_to_be_uploaded(region, bucket, spark_event_logs_key_prefix, file_name, file_size)
    spark.start_history_server(spark_event_logs_s3_uri=spark_event_logs_s3_uri)

    try:
        response = _request_with_retry(HISTORY_SERVER_ENDPOINT)
        assert response.status == 200

        response = _request_with_retry(f"{HISTORY_SERVER_ENDPOINT}{SPARK_APPLICATION_URL_SUFFIX}", max_retries=15)
        print(f"Subpage response status code: {response.status}")
    finally:
        spark.terminate_history_server()


def test_history_server_with_expected_failure(tag, role, image_uri, sagemaker_session, caplog):
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        framework_version=tag,
        image_uri=image_uri,
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        max_runtime_in_seconds=1200,
        sagemaker_session=sagemaker_session,
    )

    caplog.set_level(logging.ERROR)
    spark.start_history_server(spark_event_logs_s3_uri="invalids3uri")
    response = _request_with_retry(HISTORY_SERVER_ENDPOINT, max_retries=5)
    assert response is None
    assert "History server failed to start. Please run 'docker logs history_server' to see logs" in caplog.text


def _request_with_retry(url, max_retries=10):
    http = urllib3.PoolManager(
        retries=Retry(
            max_retries,
            redirect=max_retries,
            status=max_retries,
            status_forcelist=[502, 404],
            backoff_factor=0.2,
        )
    )
    try:
        return http.request("GET", url)
    except Exception:  # pylint: disable=W0703
        return None


# due to s3 eventual consistency, s3 file may not be uploaded when we kick off history server
def _wait_for_file_to_be_uploaded(region, bucket, prefix, file_name, file_size, timeout=20):
    s3_client = boto3.client("s3", region_name=region)

    max_time = time.time() + timeout
    while True:
        response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
        if "Contents" in response:
            for file in response["Contents"]:
                if file["Key"] == f"{prefix}/{file_name}" and file["Size"] == file_size:
                    print(f"{file_name} was fully uploaded.")
                    return

        if time.time() > max_time:
            raise TimeoutError("Timeout waiting for s3 upload to be consistent")

        time.sleep(1)

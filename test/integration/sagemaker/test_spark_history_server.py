import requests.packages.urllib3 as urllib3
from requests.packages.urllib3.util.retry import Retry

from sagemaker.s3 import S3Uploader
from unittest.mock import patch
from sagemaker.spark.processing import PySparkProcessor

HISTORY_SERVER_ENDPOINT = "http://0.0.0.0/proxy/15050"
SPARK_APPLICATION_URL_SUFFIX = "/history/application_1594922484246_0001/1/jobs/"


def test_history_server(tag, role, image_uri, sagemaker_session):
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

    with open("test/resources/data/files/sample_spark_event_logs") as data:
        body = data.read()
        S3Uploader.upload_string_as_file_body(
            body=body,
            desired_s3_uri=spark_event_logs_s3_uri + "/sample_spark_event_logs",
            sagemaker_session=sagemaker_session,
        )

    spark.start_history_server(spark_event_logs_s3_uri=spark_event_logs_s3_uri)

    try:
        response = _request_with_retry(HISTORY_SERVER_ENDPOINT)
        assert response.status == 200

        # spark has redirect behavior, this request verify that page navigation works with redirect
        response = _request_with_retry(f"{HISTORY_SERVER_ENDPOINT}{SPARK_APPLICATION_URL_SUFFIX}")
        assert response.status == 200

        html_content = response.data.decode("utf-8")
        assert "Completed Jobs (4)" in html_content
        assert "collect at /opt/ml/processing/input/code/test_long_duration.py:32" in html_content
    finally:
        spark.terminate_history_server()


@patch("sagemaker.spark.processing.print")
def test_integ_history_server_with_expected_failure(mock_print, tag, role, image_uri):
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        framework_version=tag,
        image_uri=image_uri,
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        max_runtime_in_seconds=1200,
    )

    spark.start_history_server(spark_event_logs_s3_uri="invalids3uri")
    response = _request_with_retry(HISTORY_SERVER_ENDPOINT, max_retries=5)
    assert response is None
    mock_print.assert_called_with("History server failed to start. Please run 'docker logs history_server' to see logs")


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

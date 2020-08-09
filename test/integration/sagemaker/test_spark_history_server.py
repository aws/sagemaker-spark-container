import urllib.request

from sagemaker.s3 import S3Uploader
from sagemaker.spark.processing import PySparkProcessor


def test_history_server(tag, role, image_uri):
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        framework_version=tag,
        image_uri=image_uri,
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        max_runtime_in_seconds=1200,
    )
    bucket = spark.sagemaker_session.default_bucket()
    spark_event_logs_key_prefix = "spark/spark-history-fs"
    spark_event_logs_s3_uri = "s3://{}/{}".format(bucket, spark_event_logs_key_prefix)

    with open("test/resources/data/files/sample_spark_event_logs") as data:
        body = data.read()
        S3Uploader.upload_string_as_file_body(body=body, desired_s3_uri=spark_event_logs_s3_uri + "/sample_spark_event_logs")

    spark.start_history_server(spark_event_logs_s3_uri=spark_event_logs_s3_uri)
    response = urllib.request.urlopen("http://0.0.0.0/proxy/15050")
    assert response.status == 200

    # spark has redirect behavior, this request verify that page navigation works with redirect
    response = urllib.request.urlopen("http://0.0.0.0/proxy/15050/history/application_1594922484246_0001/1/jobs/")
    assert response.status == 200

    html_content = response.read().decode("UTF-8")
    assert "Completed Jobs (4)" in html_content
    assert "collect at /opt/ml/processing/input/code/test_long_duration.py:32" in html_content

    spark.terminate_history_server()
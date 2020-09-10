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
            submit_app_py="test/resources/code/python/py_spark_app_error/py_spark_app_error.py", wait=True, logs=False,
        )
    except Exception:
        pass  # this job is expected to fail
    processing_job = spark.latest_job

    describe_response = processing_job.describe()
    assert "AlgorithmError: See job logs for more information" == describe_response["FailureReason"]
    assert "Algorithm Error: (caused by CalledProcessError)" in describe_response["ExitMessage"]
    assert "returned non-zero exit status 1" in describe_response["ExitMessage"]

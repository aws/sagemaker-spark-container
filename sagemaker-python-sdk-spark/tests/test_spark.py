import pytest
from sagemaker.spark.processing import PySparkProcessor


def test_pyspark_processor_instantiation():
    # This just tests that the import is right and that the processor can be instantiated
    # Functionality is tested in project root container directory.
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        role="AmazonSageMaker-ExecutionRole",
        framework_version="0.1.0",
        instance_count=1,
        instance_type="ml.c5.xlarge",
    )


happy_config_dict = {
    "Classification": "core-site",
    "Properties": {"hadoop.security.groups.cache.secs": "250"},
}
happy_config_list = [
    {"Classification": "core-site", "Properties": {"hadoop.security.groups.cache.secs": "250"}},
    {"Classification": "spark-defaults", "Properties": {"spark.driver.memory": "2"}},
]
nested_config = [
    {
        "Classification": "yarn-env",
        "Properties": {},
        "Configurations": [
            {
                "Classification": "export",
                "Properties": {
                    "YARN_RESOURCEMANAGER_OPTS": "-Xdebug -Xrunjdwp:transport=dt_socket"
                },
                "Configurations": [],
            }
        ],
    }
]
invalid_classification_dict = {"Classification": "invalid-site", "Properties": {}}
invalid_classification_list = [invalid_classification_dict]
missing_classification_dict = {"Properties": {}}
missing_classification_list = [missing_classification_dict]
missing_properties_dict = {"Classification": "core-site"}
missing_properties_list = [missing_properties_dict]


@pytest.mark.parametrize(
    "config,expected",
    [
        (happy_config_dict, None),
        (invalid_classification_dict, ValueError),
        (happy_config_list, None),
        (invalid_classification_list, ValueError),
        (nested_config, None),
        (missing_classification_dict, ValueError),
        (missing_classification_list, ValueError),
        (missing_properties_dict, ValueError),
        (missing_properties_list, ValueError),
    ],
)
def test_configuration_validation(config, expected) -> None:
    # This just tests that the import is right and that the processor can be instantiated
    # Functionality is tested in project root container directory.
    spark = PySparkProcessor(
        base_job_name="sm-spark",
        role="AmazonSageMaker-ExecutionRole",
        framework_version="0.1.0",
        instance_count=1,
        instance_type="ml.c5.xlarge",
    )

    if expected is None:
        spark._validate_configuration(config)
    else:
        with pytest.raises(expected):
            spark._validate_configuration(config)
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
import boto3
import botocore
import botocore.exceptions
import pytest
import logging
import time
from sagemaker.session import Session


def pytest_addoption(parser) -> str:
    parser.addoption("--role")
    parser.addoption("--image_uri")
    parser.addoption("--account-id")
    parser.addoption("--region", default="us-west-2")
    parser.addoption("--repo")
    parser.addoption("--tag")
    parser.addoption("--spark-version")
    parser.addoption("--framework-version")
    parser.addoption("--domain", default="amazonaws.com")


@pytest.fixture(scope="session")
def image_uri(request, account_id, region, repo, tag, domain) -> str:
    """Return image uri for use in tests"""
    return request.config.getoption("--image_uri") or f"{account_id}.dkr.ecr.{region}.{domain}/{repo}:{tag}"


@pytest.fixture(scope="session")
def account_id(request, region, boto_session, domain) -> str:
    """Return account-id if provided, otherwise defaults to caller's account ID when using ECR's account_id"""
    if request.config.getoption("--account-id"):
        return request.config.getoption("--account-id")

    sts = boto_session.client("sts", region_name=region, endpoint_url=f"https://sts.{region}.{domain}")
    return sts.get_caller_identity()["Account"]


@pytest.fixture(scope="session")
def region(request) -> str:
    """Return region, such as us-west-2, for use in tests."""
    return request.config.getoption("--region") or "us-west-2"


@pytest.fixture(scope="session")
def partition(region) -> str:
    """Return partition, such as aws, aws-cn, for use in tests."""
    region_partition_amp = {
        "us-gov-west-1": "aws-us-gov",
        "cn-north-1": "aws-cn",
        "cn-northwest-1": "aws-cn",
    }

    return region_partition_amp.get(region, "aws")


@pytest.fixture(scope="session")
def repo(request) -> str:
    """Return ECR repository to use in tests."""
    return request.config.getoption("--repo")


@pytest.fixture(scope="session")
def tag(request) -> str:
    """Return Docker image tag to use in tests."""
    return request.config.getoption("--tag")


@pytest.fixture(scope="session")
def spark_version(request) -> str:
    """Return Docker image framework_version to use in tests."""
    return request.config.getoption("--spark-version")


@pytest.fixture(scope="session")
def framework_version(request) -> str:
    """Return Docker image framework_version to use in tests."""
    return request.config.getoption("--framework-version")


@pytest.fixture(scope="session")
def domain(request) -> str:
    """Return AWS domain"""
    return request.config.getoption("--domain") or "amazonaws.com"


@pytest.fixture(scope="session")
def role(request) -> str:
    """Return the job execution role to use in a test"""
    return request.config.getoption("--role")


@pytest.fixture(scope="session")
def boto_session(region) -> boto3.session.Session:
    """Return a boto session for use in constructing clients in integration tests."""
    return boto3.Session(region_name=region)


@pytest.fixture(scope="session")
def sagemaker_client(boto_session, region) -> botocore.client.BaseClient:
    """Return a SageMaker client for use in integration tests."""
    return boto_session.client("sagemaker", region_name=region)


@pytest.fixture(scope="session")
def sagemaker_session(boto_session, sagemaker_client) -> Session:
    """Return a SageMaker session for use in integration tests."""
    return Session(boto_session=boto_session, sagemaker_client=sagemaker_client)


@pytest.fixture(scope="session")
def is_feature_store_available(region) -> bool:
    """Check if feature store is available in current region."""
    sagemaker_client = boto3.client("sagemaker")
    max_attempts = 4
    retry_num = 0
    while retry_num < max_attempts:
        try:
            sagemaker_client.list_feature_groups()
            return True
        except botocore.exceptions.EndpointConnectionError:
            logging.info(
                "Caught EndpointConnectionError when checking the feature store availability,"
                " retried %d times already",
                retry_num,
            )
            retry_num += 1
            time.sleep(5)

    logging.info("Feature store is not available in %s.", region)
    return False

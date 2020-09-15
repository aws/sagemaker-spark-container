import boto3
import botocore
import pytest
from sagemaker.session import Session


def pytest_addoption(parser) -> str:
    parser.addoption("--role")
    parser.addoption("--image_uri")
    parser.addoption("--account-id")
    parser.addoption("--region", default="us-west-2")
    parser.addoption("--repo")
    parser.addoption("--tag")
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
def repo(request) -> str:
    """Return ECR repository to use in tests."""
    return request.config.getoption("--repo")


@pytest.fixture(scope="session")
def tag(request) -> str:
    """Return Docker image tag to use in tests."""
    return request.config.getoption("--tag")


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

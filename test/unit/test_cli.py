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
import pathlib
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Generator, List, Type, Union
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner
from smspark.cli import submit, submit_main
from smspark.errors import InputError
from smspark.job import ProcessingJobManager


@pytest.fixture
def tempdir() -> Generator[tempfile.TemporaryDirectory, None, None]:
    dir = tempfile.TemporaryDirectory()
    yield dir
    dir.cleanup()


@pytest.fixture
def empty_tempdir_path() -> Generator[str, None, None]:
    dir = tempfile.TemporaryDirectory()
    yield pathlib.Path(dir.name).resolve()
    dir.cleanup()


@pytest.fixture
def jars_dir(tempdir: tempfile.TemporaryDirectory) -> str:
    return tempdir.name


@pytest.fixture
def jar_file(jars_dir: str) -> Generator[str, None, None]:
    tmp = tempfile.NamedTemporaryFile(dir=jars_dir, prefix="1", suffix=".jar")
    yield pathlib.Path(tmp.name).resolve()
    tmp.close()


@pytest.fixture
def other_jar_file(jars_dir: str) -> Generator[str, None, None]:
    tmp = tempfile.NamedTemporaryFile(dir=jars_dir, prefix="2", suffix=".jar")
    yield pathlib.Path(tmp.name).resolve()
    tmp.close()


@dataclass
class SubmitTest:
    """Represents data for one submit test."""

    name: str
    args: str
    expected_cmd: Union[str, Type[BaseException]]


def get_test_cases() -> List[SubmitTest]:
    test_cases = []
    for arg in ["--jars", "--files", "--py-files"]:
        files_test_cases = [
            SubmitTest(
                name="single local jar should pass",
                args=arg + " {jar_file} app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client " + arg + " {jar_file} app.jar",
            ),
            SubmitTest(
                name="list of local jars should pass",
                args=arg + " {jar_file},{other_jar_file} app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client "
                + arg
                + " {jar_file},{other_jar_file} app.jar",
            ),
            SubmitTest(
                name="s3 url to jar should pass",
                args=arg + " s3://bucket/to/jar1.jar app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client "
                + arg
                + " s3://bucket/to/jar1.jar app.jar",
            ),
            SubmitTest(
                name="s3a url to jar should pass",
                args=arg + " s3a://bucket/to/jar1.jar app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client "
                + arg
                + " s3a://bucket/to/jar1.jar app.jar",
            ),
            SubmitTest(
                name="multiple s3 urls to jar should pass",
                args=arg + " s3://bucket/to/jar1.jar,s3://bucket/to/jar2.jar app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client "
                + arg
                + " s3://bucket/to/jar1.jar,s3://bucket/to/jar2.jar app.jar",
            ),
            SubmitTest(
                name="mixed s3 urls to jars and local paths should pass",
                args=arg + " s3://bucket/to/jar1.jar,{jar_file} app.jar",
                expected_cmd="spark-submit --master yarn --deploy-mode client "
                + arg
                + " s3://bucket/to/jar1.jar,{jar_file} app.jar",
            ),
            SubmitTest(
                name="relative paths should fail",
                args=arg + " relative/path/to/jar.jar app.jar",
                expected_cmd=InputError,
            ),
            SubmitTest(
                name="nonexistent paths should fail",
                args=arg + " /path/to/nonexistent/file app.jar",
                expected_cmd=InputError,
            ),
            SubmitTest(
                name="directory with no files should fail",
                args=arg + " {empty_tempdir_path} app.jar",
                expected_cmd=InputError,
            ),
        ]
        test_cases = test_cases + files_test_cases

    test_cases = [
        SubmitTest(name="missing APP arg should fail", args="", expected_cmd=click.exceptions.MissingParameter,),
        SubmitTest(
            name="invalid spark options should fail",
            args="--invalid-spark-option opt arg.py",
            expected_cmd=click.exceptions.NoSuchOption,
        ),
        SubmitTest(
            name="happy path should pass",
            args="app.py",
            expected_cmd="spark-submit --master yarn --deploy-mode client app.py",
        ),
        SubmitTest(
            name="valid spark option should pass",
            args="--class com.app.Main app.jar",
            expected_cmd="spark-submit --master yarn --deploy-mode client --class com.app.Main app.jar",
        ),
    ] + test_cases

    # Quote tests:

    test_cases.append(
        SubmitTest(
            name="quotes are handled correctly",
            args="--jars {jar_file} myscript.py --query-bbox 'BBOX(geometry,0.1,-0.2,3.3,4.5)' --query-start-date '2020-05-01 00:00:00' --query-end-date '2020-05-31 23:59:59' --data-location-uri s3://123456789012-us-west-2/path --bucket-region us-west-2 --output-folder-path /opt/ml/processing/output/out --output-stage gamma --storage-bucket-uri s3://123456789012-us-west-2/",
            expected_cmd="spark-submit --master yarn --deploy-mode client --jars {jar_file} myscript.py --query-bbox 'BBOX(geometry,0.1,-0.2,3.3,4.5)' --query-start-date '2020-05-01 00:00:00' --query-end-date '2020-05-31 23:59:59' --data-location-uri s3://123456789012-us-west-2/path --bucket-region us-west-2 --output-folder-path /opt/ml/processing/output/out --output-stage gamma --storage-bucket-uri s3://123456789012-us-west-2/",
        )
    )

    return test_cases


test_cases = get_test_cases()


@patch("smspark.cli.ProcessingJobManager")
@pytest.mark.parametrize("test_case", test_cases, ids=[submit_test.name for submit_test in test_cases])
def test_submit(
    patched_processing_job_manager: ProcessingJobManager,
    test_case: SubmitTest,
    jar_file: str,
    other_jar_file: str,
    empty_tempdir_path: str,
) -> None:
    runner = CliRunner()

    args = test_case.args.format(
        jar_file=jar_file, other_jar_file=other_jar_file, empty_tempdir_path=empty_tempdir_path
    )
    result = runner.invoke(submit, args, standalone_mode=False)

    # happy
    if isinstance(test_case.expected_cmd, str):
        expected_cmd = test_case.expected_cmd.format(jar_file=jar_file, other_jar_file=other_jar_file)
        assert result.exception is None, result.output
        assert result.exit_code == 0
        patched_processing_job_manager.assert_called_once()
        patched_processing_job_manager.return_value.run.assert_called_once_with(expected_cmd, None, None)

    # sad
    else:
        assert result.exit_code != 0, result.output
        assert isinstance(result.exception, test_case.expected_cmd)

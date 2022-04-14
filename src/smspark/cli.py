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
"""Entry-point for command-line commands."""
import logging
import pathlib
import shlex
import sys
from typing import Any, Dict, Sequence
from urllib.parse import urlparse

import click
from smspark.errors import AlgorithmError, BaseError, InputError
from smspark.job import ProcessingJobManager

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command(context_settings=dict(allow_interspersed_args=False))
@click.option("--class", "class_", help="Fully-qualified name to your application's main class.")
@click.option(
    "--jars",
    help="Either a directory or a comma-separated list of jars to include on the driver and executor classpaths."
    + " If a directory is given, each file in the directory and its subdirectories is included.",
)
@click.option(
    "--py-files",
    help="Either a directory or a comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH."
    + " If a directory is provided, each file in the directory and its subdirectories is included.",
)
@click.option(
    "--files",
    help="Either a directory or a comma-separated list of files to be placed in the working directory of each executor."
    + " If a directory is provided, each file in the directory and its subdirectories is included.",
)
@click.option(
    "--spark-event-logs-s3-uri",
    help="Optional, spark events file will be published to this s3 destination",
)
@click.option(
    "--local-spark-event-logs-dir",
    help="Optional, spark events will be stored in this local path",
)
@click.option("-v", "--verbose", is_flag=True, help="Print additional debug output.")
@click.argument("app")
@click.argument("app_arguments", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def submit(
    ctx: click.Context,
    class_: str,
    jars: str,
    py_files: str,
    files: str,
    spark_event_logs_s3_uri: str,
    local_spark_event_logs_dir: str,
    verbose: bool,
    app: str,
    app_arguments: str,
) -> None:
    """Submit a job to smspark-submit.

    smspark-submit generally passes options through to spark-submit, but with the exception of certain options,
    including "--jars", --py-files", and "--files", which extend on spark-submit's equivalents to allow a directory
    to be specified rather than only a comma-separated list of URIs.

    Not all spark-submit options are enabled with smspark-submit.
    Options that configure Spark, such as "--conf", "--properties-file", and "--driver-memory" are missing, for example.
    Users are recommended to configure values for Spark, Hadoop, and Hive through the unified configuration interface
    rather than smspark-submit.

    Like spark-submit, smspark-submit requires requires a path to a Python or JAR file implementing the Spark
    application. R applications are not supported. Any trailing arguments are passed through without modification
    as application arguments.
    """
    log.info(f"Parsing arguments. argv: {sys.argv}")

    # Parse smspark-submit options which will be passed to spark-submit as spark options.
    spark_options: Dict[str, Any] = ctx.params
    # Ignore application arguments, which will be passed through verbatim to spark-submit.
    spark_options.pop("app", None)
    spark_options.pop("app_arguments", None)
    spark_options.pop("spark_event_logs_s3_uri", None)
    spark_options.pop("local_spark_event_logs_dir", None)
    app_and_app_arguments = [app] + list(app_arguments)

    # args needs to be a dict. remaining_args should be a list of [app app_arguments]
    log.info("Raw spark options before processing: {}".format(spark_options))
    log.info("App and app arguments: {}".format(app_and_app_arguments))

    # Ensure that --jars, --files, and --py-files are comma-delimited lists of URIs
    rendered_spark_options = _render_spark_opts(spark_options)
    log.info("Rendered spark options: {}".format(rendered_spark_options))

    # Compile the spark-submit command.
    spark_submit_cmd = _construct_spark_submit_command(
        spark_opts=rendered_spark_options, app_and_app_arguments=app_and_app_arguments
    )

    # Run the job.
    log.info("Initializing processing job.")
    processing_job_manager = ProcessingJobManager()

    log.info(f"running spark submit command: {spark_submit_cmd}")
    processing_job_manager.run(spark_submit_cmd, spark_event_logs_s3_uri, local_spark_event_logs_dir)


def submit_main() -> None:
    """Run main and handle errors."""
    try:
        submit.main(standalone_mode=False)
        sys.exit(0)
    except click.exceptions.MissingParameter as e:
        # "ValueError: missing parameter: app"
        AlgorithmError(message="Couldn't parse input.", caused_by=e).log_and_exit()
    except click.exceptions.NoSuchOption as e:
        # "ValueError: no such option: --nonexistent"
        AlgorithmError(message="Couldn't parse input.", caused_by=e).log_and_exit()
    except Exception as e:
        if isinstance(e, BaseError):
            e.log_and_exit()
        else:
            AlgorithmError(message="error running Spark job", caused_by=e).log_and_exit()


def _render_spark_opts(spark_opts: Dict[str, Any]) -> Dict[str, Any]:
    """Render spark-submit options.

    Args:
        spark_opts (Dict[str, str]): Dictionary storing spark options and values.

    Returns:
        Dict[str, str]: Dictionary with finalized, rendered spark options and values.
    """
    if "jars" in spark_opts and spark_opts["jars"]:
        spark_opts["jars"] = _get_list_of_files(spark_opts["jars"])
    if "files" in spark_opts and spark_opts["files"]:
        spark_opts["files"] = _get_list_of_files(spark_opts["files"])
    if "py_files" in spark_opts and spark_opts["py_files"]:
        spark_opts["py_files"] = _get_list_of_files(spark_opts["py_files"])

    return spark_opts


def _get_list_of_files(path_str: str) -> str:
    """Expand an absolute path into a comma-delimited list of files under that path.

    Certain smspark-submit options (--jars, --files, --py-files) may be a path
    to a directory containing jars, files, or python files, whereas spark-submit
    expects a comma-delimited list of absolute file paths.

    For example, given a namespace with an attribute "jars" and value "/path/to/jars/dir",
    this function returns ["--jars", "/path/to/jars/dir/file1.jar,/path/to/jars/dir/file2.jar"].
    Since the value of the "jars" option is now a comma-delimited list of file paths rather
    than a directory, it's a valid input for spark-submit's "--jars" option.

    If the given path is not a directory, the path is returned as is.
    For example, given --jars "/path/to/my/jar1.jar,/path/to/my/jar2.jar", this
    function returns the path as-given.

    Includes top-level files, as well as files found recursively in subdirectories
    of the given path.
    """
    if not path_str:
        raise InputError(ValueError(f"path {path_str} must not be empty"))

    paths = path_str.split(",")
    expanded_paths = []
    for path in paths:
        result = urlparse(path_str)
        if result.scheme == "s3" or result.scheme == "s3a":
            expanded_paths.append(path)
        elif result.scheme == "file" or not result.scheme:
            file_path = pathlib.Path(path)
            if not file_path.is_absolute():
                raise InputError(ValueError(f"file path {file_path} must be an absolute path to a file or directory"))

            file_path = file_path.resolve()

            # In the typical case, file_path points to a directory containing files.
            if not file_path.exists():
                raise InputError(ValueError(f"file path {file_path} does not exist"))

            if file_path.is_dir():
                files = [str(f.resolve()) for f in file_path.iterdir() if f.is_file()]
                if not files:
                    raise InputError(ValueError(f"Found zero files in {file_path}"))
                for f in files:
                    expanded_paths.append(f)
            elif file_path.is_file():
                expanded_paths.append(str(file_path))
            else:
                raise InputError(ValueError(f"file at {file_path} is not a regular file or directory"))

    return ",".join(expanded_paths)


def _construct_spark_submit_command(spark_opts: Dict[str, Any], app_and_app_arguments: Sequence[str]) -> str:
    """Construct a spark-submit command from smspark-submit options, app, and app arguments.

    Args:
        spark_opts (Sequence[str]): A sequence of strings to pass to spark-submit.
        app_and_app_arguments (Sequence[str]): A sequence of strings consisting of the application jar or file,
                                        and application arguments.
    Returns:
        str: The string to be used as the spark-submit command.
    """
    spark_options_list = []
    for key, val in spark_opts.items():
        # "-v" or "--verbose" is an option flag, unlike the long options, which require a value.
        if key == "verbose" and val is True:
            spark_options_list.append("--verbose")
            continue
        # Undo collision between python built-in "class" and spark "--class" option.
        if key == "class_":
            key = "class"

        # Python converts hyphenated options ("py-files") to underscored attributes ("py_files"). Undo underscores.
        if "_" in key:
            key = key.replace("_", "-")
        if val:
            spark_options_list.append(f"--{key}")
            spark_options_list.append(val)
    cmd = ["spark-submit", "--master", "yarn", "--deploy-mode", "client"]
    cmd.extend(spark_options_list)
    cmd.extend(app_and_app_arguments)
    cmd_string = " ".join(shlex.quote(c) for c in cmd)
    return cmd_string

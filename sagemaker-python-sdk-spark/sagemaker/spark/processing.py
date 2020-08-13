# Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
"""This module contains code related to Spark Processors, which are used
for Processing jobs. These jobs let customers perform data pre-processing,
post-processing, feature engineering, data validation, and model evaluation
on SageMaker using Spark and PySpark.
"""
from __future__ import absolute_import

import json
import os.path
import shutil
import subprocess
import tempfile
import time
import urllib.request
from io import BytesIO
from urllib.parse import urlparse

from sagemaker.processing import (ProcessingInput, ProcessingOutput,
                                  ScriptProcessor)
from sagemaker.s3 import S3Uploader
from sagemaker.session import Session


class _SparkProcessorBase(ScriptProcessor):
    """Handles Amazon SageMaker processing tasks for jobs using Spark. Base class for either PySpark or SparkJars."""

    _framework_versions = {"0.1": "254171628281", "0.1.0": "254171628281"}

    _default_command = "smspark-submit"
    _image_uri_format = "{}.dkr.ecr.{}.amazonaws.com/{}:{}"
    _CONF_CONTAINER_BASE_PATH = "/opt/ml/processing/input/"
    _CONF_CONTAINER_INPUT_NAME = "conf"
    _CONF_FILE_NAME = "configuration.json"
    _valid_configuration_keys = ["Classification", "Properties", "Configurations"]
    _valid_configuration_classifications = [
        "core-site",
        "hadoop-env",
        "hadoop-log4j",
        "hive-env",
        "hive-log4j",
        "hive-exec-log4j",
        "hive-site",
        "spark-defaults",
        "spark-env",
        "spark-log4j",
        "spark-hive-site",
        "spark-metrics",
        "yarn-env",
        "yarn-site",
        "export",
    ]

    _SUBMIT_JARS_INPUT_CHANNEL_NAME = "jars"
    _SUBMIT_FILES_INPUT_CHANNEL_NAME = "files"
    _SUBMIT_PY_FILES_INPUT_CHANNEL_NAME = "py-files"
    _SUBMIT_DEPS_ERROR_MESSAGE = (
        "Please specify a list of one or more S3 URIs, local file paths, and/or local directory paths"
    )

    # history server vars
    _HISTORY_SERVER_TIMEOUT = 20
    _HISTORY_SERVER_URL_SUFFIX = "/proxy/15050"
    _SPARK_EVENT_LOG_DEFAULT_LOCAL_PATH = "/opt/ml/processing/spark-events/"

    def __init__(
        self,
        role,
        instance_type,
        instance_count,
        framework_version=None,
        image_uri=None,
        command=None,
        volume_size_in_gb=30,
        volume_kms_key=None,
        output_kms_key=None,
        max_runtime_in_seconds=None,
        base_job_name=None,
        sagemaker_session=None,
        env=None,
        tags=None,
        network_config=None,
    ):
        """Initialize a ``_SparkProcessorBase`` instance. The _SparkProcessorBase
        handles Amazon SageMaker processing tasks for jobs using SageMaker Spark.

        Args:
            framework_version (str): The version of SageMaker PySpark.
            role (str): An AWS IAM role name or ARN. The Amazon SageMaker training jobs
                and APIs that create Amazon SageMaker endpoints use this role
                to access training data and model artifacts. After the endpoint
                is created, the inference code might use the IAM role, if it
                needs to access an AWS resource.
            instance_type (str): Type of EC2 instance to use for
                processing, for example, 'ml.c4.xlarge'.
            instance_count (int): The number of instances to run
                the Processing job with. Defaults to 1.
            command ([str]): The command to run, along with any command-line flags.
                Example: ["python3", "-v"]. If not provided, ["python3"] or ["python2"]
                will be chosen based on the py_version parameter.
            volume_size_in_gb (int): Size in GB of the EBS volume to
                use for storing data during processing (default: 30).
            volume_kms_key (str): A KMS key for the processing
                volume.
            output_kms_key (str): The KMS key id for all ProcessingOutputs.
            max_runtime_in_seconds (int): Timeout in seconds.
                After this amount of time Amazon SageMaker terminates the job
                regardless of its current status.
            base_job_name (str): Prefix for processing name. If not specified,
                the processor generates a default job name, based on the
                training image name and current timestamp.
            sagemaker_session (sagemaker.session.Session): Session object which
                manages interactions with Amazon SageMaker APIs and any other
                AWS services needed. If not specified, the processor creates one
                using the default AWS configuration chain.
            env (dict): Environment variables to be passed to the processing job.
            tags ([dict]): List of tags to be passed to the processing job.
            network_config (sagemaker.network.NetworkConfig): A NetworkConfig
                object that configures network isolation, encryption of
                inter-container traffic, security group IDs, and subnets.
        """
        self.history_server = None
        session = sagemaker_session or Session()
        region = session.boto_region_name

        if not image_uri:
            account_id = _SparkProcessorBase._framework_versions[framework_version]
            image_uri = _SparkProcessorBase._image_uri_format.format(
                account_id, region, "sagemaker-spark", framework_version
            )

        if not env:
            env = {}

        if not command:
            command = [_SparkProcessorBase._default_command]

        super(_SparkProcessorBase, self).__init__(
            role=role,
            image_uri=image_uri,
            instance_count=instance_count,
            instance_type=instance_type,
            command=command,
            volume_size_in_gb=volume_size_in_gb,
            volume_kms_key=volume_kms_key,
            output_kms_key=output_kms_key,
            max_runtime_in_seconds=max_runtime_in_seconds,
            base_job_name=base_job_name,
            sagemaker_session=session,
            env=env,
            tags=tags,
            network_config=network_config,
        )

    def run(
        self,
        submit_app,
        inputs=None,
        outputs=None,
        arguments=None,
        wait=True,
        logs=True,
        job_name=None,
        experiment_config=None,
        configuration=None,
        spark_event_logs_s3_uri=None,
    ):
        """Runs a processing job.
        Args:
            submit_app (str): .py or .jar file to submit to Spark as the primary application
            inputs (list[:class:`~sagemaker.processing.ProcessingInput`]): Input files for
                the processing job. These must be provided as
                :class:`~sagemaker.processing.ProcessingInput` objects (default: None).
            outputs (list[:class:`~sagemaker.processing.ProcessingOutput`]): Outputs for
                the processing job. These can be specified as either path strings or
                :class:`~sagemaker.processing.ProcessingOutput` objects (default: None).
            arguments (list[str]): A list of string arguments to be passed to a
                processing job (default: None).
            wait (bool): Whether the call should wait until the job completes (default: True).
            logs (bool): Whether to show the logs produced by the job.
                Only meaningful when wait is True (default: True).
            job_name (str): Processing job name. If not specified, the processor generates
                a default job name, based on the base job name and current timestamp.
            experiment_config (dict[str, str]): Experiment management configuration.
                Dictionary contains three optional keys:
                'ExperimentName', 'TrialName', and 'TrialComponentDisplayName'.
            configuration (list[dict] or dict): Configuration for Hadoop, Spark, or Hive.
                List or dictionary of EMR-style classifications.
                https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
            spark_event_logs_s3_uri (str): S3 path where spark application events will
                be published to.
        """
        self._current_job_name = self._generate_current_job_name(job_name=job_name)

        if spark_event_logs_s3_uri:
            self._validate_s3_uri(spark_event_logs_s3_uri)

            # SPARK_LOCAL_EVENT_LOG_DIR will be passed to container to write spark event log to local file
            # SPARK_EVENT_LOGS_S3_URI can be used to start history server
            self.env["SPARK_LOCAL_EVENT_LOG_DIR"] = _SparkProcessorBase._SPARK_EVENT_LOG_DEFAULT_LOCAL_PATH
            self.env[_HistoryServer.ARG_EVENT_LOGS_S3_URI] = spark_event_logs_s3_uri

            output = ProcessingOutput(
                source=_SparkProcessorBase._SPARK_EVENT_LOG_DEFAULT_LOCAL_PATH,
                destination=spark_event_logs_s3_uri,
                s3_upload_mode="Continuous",
            )

            if outputs:
                outputs.append(output)
            else:
                outputs = [output]

        if configuration:
            self._validate_configuration(configuration)
            if not inputs:
                inputs = []
            inputs.append(self._stage_configuration(configuration))

        super().run(
            submit_app, inputs, outputs, arguments, wait, logs, job_name, experiment_config,
        )

    # TODO (guoqiao@): support multiple history servers on notebook instance
    def start_history_server(self, spark_event_logs_s3_uri=None):
        """
        :param spark_event_logs_s3_uri (str): optional parameter to set s3 uri and run history server
        """
        self._pull_docker_container()
        history_server_env_variables = self._prepare_history_server_env_variables(spark_event_logs_s3_uri)
        self.history_server = _HistoryServer(history_server_env_variables, self.image_uri)
        process = self.history_server.run()
        # print history url when it's ready
        self._check_history_server(process)

    def terminate_history_server(self):
        if self.history_server:
            print("History server is running, terminating history server")
            self.history_server.down()
            self.history_server = None

    def _validate_configuration(self, configuration):
        """Validate the user-provided Hadoop/Spark/Hive configuration.

        This ensures that the list or dictionary the user provides will serialize to JSON matching the
        schema of EMR's application configuration:

        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
        """
        emr_configure_apps_url = "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html"
        if isinstance(configuration, dict):
            keys = configuration.keys()
            if "Classification" not in keys or "Properties" not in keys:
                raise ValueError(
                    "Missing one or more required keys in configuration dictionary {}. Please see {} for more information",
                    configuration,
                    emr_configure_apps_url,
                )

            for key in keys:
                if key not in _SparkProcessorBase._valid_configuration_keys:
                    raise ValueError(
                        "Invalid key: {}. Must be one of {}. Please see {} for more information.".format(
                            key, _SparkProcessorBase._valid_configuration_keys, emr_configure_apps_url
                        )
                    )
                if key == "Classification":
                    if configuration[key] not in _SparkProcessorBase._valid_configuration_classifications:
                        raise ValueError(
                            "Invalid classification: {}. Must be one of {}".format(
                                key, _SparkProcessorBase._valid_configuration_classifications
                            )
                        )

        if isinstance(configuration, list):
            for item in configuration:
                self._validate_configuration(item)

    def _stage_configuration(self, configuration):
        """Serialize and upload the user-provided EMR application configuration to S3

        This method prepares an input channel
        """

        serialized_configuration = BytesIO(json.dumps(configuration).encode("utf-8"))
        s3_uri = "s3://{}/{}/input/{}/{}".format(
            self.sagemaker_session.default_bucket(),
            self._current_job_name,
            _SparkProcessorBase._CONF_CONTAINER_INPUT_NAME,
            _SparkProcessorBase._CONF_FILE_NAME,
        )

        S3Uploader.upload_string_as_file_body(body=serialized_configuration, desired_s3_uri=s3_uri, sagemaker_session=self.sagemaker_session)

        conf_input = ProcessingInput(
            source=s3_uri,
            destination="{}{}".format(
                _SparkProcessorBase._CONF_CONTAINER_BASE_PATH, _SparkProcessorBase._CONF_CONTAINER_INPUT_NAME,
            ),
            input_name=_SparkProcessorBase._CONF_CONTAINER_INPUT_NAME,
        )
        return conf_input

    def _stage_submit_deps(self, submit_deps, input_channel_name):
        """Prepares a list of paths to jars, py-files, or files dependencies to provide as `spark-submit` options

        The submit_deps list may include a combination of S3 URIs and local paths.
        Any S3 URIs are appended to the `spark-submit` option value without modification.
        Any local file paths are copied to a temp directory, uploaded to a default S3 URI, and included
            as a ProcessingInput channel to provide as local files to the SageMaker Spark container.

        :param submit_deps (list[str]): list of one or more dependency paths to include
        :param input_channel_name (str): the `spark-submit` option name associated with the input channel
        :return (Optional[ProcessingInput], str): Tuple of (left) optional ProcessingInput for the input channel,
                    and (right) comma-delimited value for `spark-submit` option
        """
        if not submit_deps:
            raise ValueError(
                "submit_deps value may not be empty. {}".format(_SparkProcessorBase._SUBMIT_DEPS_ERROR_MESSAGE)
            )
        if not input_channel_name:
            raise ValueError("input_channel_name value may not be empty.")

        input_channel_s3_uri = "s3://{}/{}/input/{}".format(
            self.sagemaker_session.default_bucket(), self._current_job_name, input_channel_name,
        )
        use_input_channel = False
        spark_opt_s3_uris = []

        with tempfile.TemporaryDirectory() as tmpdir:
            for dep_path in submit_deps:
                dep_url = urlparse(dep_path)
                # S3 URIs are included as-is in the spark-submit argument
                if dep_url.scheme == "s3" or dep_url.scheme == "s3a":
                    spark_opt_s3_uris.append(dep_path)
                # Local files are copied to temp directory to be uploaded to S3
                elif not dep_url.scheme or dep_url.scheme == "file":
                    if not os.path.isfile(dep_path):
                        raise ValueError(
                            "submit_deps path {} is not a valid local file. "
                            "{}".format(_SparkProcessorBase._SUBMIT_DEPS_ERROR_MESSAGE)
                        )
                    print("Copying dependency from local path {} to tmpdir {}".format(dep_path, tmpdir))
                    shutil.copy(dep_path, tmpdir)
                else:
                    raise ValueError(
                        "submit_deps path {} references unsupported filesystem scheme: {} "
                        "{}".format(dep_path, dep_url.scheme, _SparkProcessorBase._SUBMIT_DEPS_ERROR_MESSAGE)
                    )

            # If any local files were found and copied, upload the temp directory to S3
            if os.listdir(tmpdir):
                print("Uploading dependencies from tmpdir {} to S3 {}".format(tmpdir, input_channel_s3_uri))
                S3Uploader.upload(
                    local_path=tmpdir, desired_s3_uri=input_channel_s3_uri, sagemaker_session=self.sagemaker_session
                )
                use_input_channel = True

        # If any local files were uploaded, construct a ProcessingInput to provide them to the Spark container
        # and form the spark-submit option from a combination of S3 URIs and container's local input path
        if use_input_channel:
            input_channel = ProcessingInput(
                source=input_channel_s3_uri,
                destination="{}{}".format(_SparkProcessorBase._CONF_CONTAINER_BASE_PATH, input_channel_name),
                input_name=input_channel_name,
            )
            spark_opt = ",".join(spark_opt_s3_uris + [input_channel.destination])
        # If no local files were uploaded, form the spark-submit option from a list of S3 URIs
        else:
            input_channel = None
            spark_opt = ",".join(spark_opt_s3_uris)

        return input_channel, spark_opt

    def _pull_docker_container(self):
        """Construct the docker login command with the correct credentials from ECR"""
        image_uri_split = self.image_uri.split(".")
        region = image_uri_split[3]
        account_id = image_uri_split[0]
        login_cmd = "$(aws ecr get-login --region {} --registry-ids {} --no-include-email)".format(region, account_id)
        subprocess.call(login_cmd, shell=True)

        print("Pulling spark history server image...")
        pull_cmd = "docker pull {}".format(self.image_uri)
        subprocess.call(pull_cmd, shell=True)

    def _prepare_history_server_env_variables(self, spark_event_logs_s3_uri):
        # prepare env varibles
        history_server_env_variables = {}

        if spark_event_logs_s3_uri:
            history_server_env_variables[_HistoryServer.ARG_EVENT_LOGS_S3_URI] = spark_event_logs_s3_uri
        # this variable will be previously set by run() method
        elif _HistoryServer.ARG_EVENT_LOGS_S3_URI in self.env:
            history_server_env_variables[_HistoryServer.ARG_EVENT_LOGS_S3_URI] = self.env[
                _HistoryServer.ARG_EVENT_LOGS_S3_URI
            ]
        else:
            raise ValueError(
                "SPARK_EVENT_LOGS_S3_URI not present. You can specify spark_event_logs_s3_uri either in"
                + "run() or start_history_server()"
            )

        if self._is_notebook_instance():
            history_server_env_variables[_HistoryServer.ARG_REMOTE_DOMAIN_NAME] = self._get_instance_domain()
        # if not notebook instance, customer need to set their own credentials
        else:
            history_server_env_variables.update(self._config_aws_credentials())

        return history_server_env_variables

    def _is_notebook_instance(self):
        return os.path.isfile("/opt/ml/metadata/resource-metadata.json")

    def _get_instance_domain(self):
        if self._is_notebook_instance():
            region = self.sagemaker_session.boto_region_name
            with open("/opt/ml/metadata/resource-metadata.json") as file:
                data = json.load(file)
                notebook_name = data["ResourceName"]

            return "https://{}.notebook.{}.sagemaker.aws".format(notebook_name, region)

    def _check_history_server(self, process):
        # ping port 15050 to check history server is up
        timeout = time.time() + self._HISTORY_SERVER_TIMEOUT

        while True:
            if self._is_history_server_started():
                if self._is_notebook_instance():
                    print("History server is up on " + self._get_instance_domain() + self._HISTORY_SERVER_URL_SUFFIX)
                else:
                    print("History server is up on localhost port 15050")
                break
            if time.time() > timeout:
                print("History server failed to start")
                print(process.stderr.read().decode("UTF-8"))
                break

            time.sleep(1)

    def _is_history_server_started(self):
        try:
            response = urllib.request.urlopen("http://localhost:15050")
            return response.status == 200
        except:
            return False

    # TODO (guoqioa@): method only checks urlparse scheme, need to perform deep s3 validation
    def _validate_s3_uri(self, spark_output_s3_path):
        if urlparse(spark_output_s3_path).scheme != "s3":
            raise ValueError(
                "Invalid s3 path: {}. Please enter something like s3://{bucket-name}/{folder-name}".format(
                    spark_output_s3_path
                )
            )

    def _config_aws_credentials(self):
        try:
            creds = self.sagemaker_session.boto_session.get_credentials()
            access_key = creds.access_key
            secret_key = creds.secret_key
            token = creds.token

            return {
                "AWS_ACCESS_KEY_ID": str(access_key),
                "AWS_SECRET_ACCESS_KEY": str(secret_key),
                "AWS_SESSION_TOKEN": str(token),
            }
        except Exception as e:
            print("Could not get AWS credentials: %s", e)
            return {}

class PySparkProcessor(_SparkProcessorBase):
    """Handles Amazon SageMaker processing tasks for jobs using PySpark."""

    def __init__(
        self,
        role,
        instance_type,
        instance_count,
        framework_version=None,
        image_uri=None,
        command=None,
        volume_size_in_gb=30,
        volume_kms_key=None,
        output_kms_key=None,
        max_runtime_in_seconds=None,
        base_job_name=None,
        sagemaker_session=None,
        env=None,
        tags=None,
        network_config=None,
    ):
        """Initialize an ``PySparkProcessor`` instance. The PySparkProcessor
        handles Amazon SageMaker processing tasks for jobs using SageMaker PySpark.

        Args:
            framework_version (str): The version of SageMaker PySpark.
            role (str): An AWS IAM role name or ARN. The Amazon SageMaker training jobs
                and APIs that create Amazon SageMaker endpoints use this role
                to access training data and model artifacts. After the endpoint
                is created, the inference code might use the IAM role, if it
                needs to access an AWS resource.
            instance_type (str): Type of EC2 instance to use for
                processing, for example, 'ml.c4.xlarge'.
            instance_count (int): The number of instances to run
                the Processing job with. Defaults to 1.
            command ([str]): The command to run, along with any command-line flags.
                Example: ["python3", "-v"]. If not provided, ["python3"] or ["python2"]
                will be chosen based on the py_version parameter.
            volume_size_in_gb (int): Size in GB of the EBS volume to
                use for storing data during processing (default: 30).
            volume_kms_key (str): A KMS key for the processing
                volume.
            output_kms_key (str): The KMS key id for all ProcessingOutputs.
            max_runtime_in_seconds (int): Timeout in seconds.
                After this amount of time Amazon SageMaker terminates the job
                regardless of its current status.
            base_job_name (str): Prefix for processing name. If not specified,
                the processor generates a default job name, based on the
                training image name and current timestamp.
            sagemaker_session (sagemaker.session.Session): Session object which
                manages interactions with Amazon SageMaker APIs and any other
                AWS services needed. If not specified, the processor creates one
                using the default AWS configuration chain.
            env (dict): Environment variables to be passed to the processing job.
            tags ([dict]): List of tags to be passed to the processing job.
            network_config (sagemaker.network.NetworkConfig): A NetworkConfig
                object that configures network isolation, encryption of
                inter-container traffic, security group IDs, and subnets.
        """

        super(PySparkProcessor, self).__init__(
            role=role,
            instance_count=instance_count,
            instance_type=instance_type,
            framework_version=framework_version,
            image_uri=image_uri,
            command=command,
            volume_size_in_gb=volume_size_in_gb,
            volume_kms_key=volume_kms_key,
            output_kms_key=output_kms_key,
            max_runtime_in_seconds=max_runtime_in_seconds,
            base_job_name=base_job_name,
            sagemaker_session=sagemaker_session,
            env=env,
            tags=tags,
            network_config=network_config,
        )

    def run(
        self,
        submit_app_py,
        submit_py_files=[],
        submit_jars=[],
        submit_files=[],
        inputs=None,
        outputs=None,
        arguments=None,
        wait=True,
        logs=True,
        job_name=None,
        experiment_config=None,
        configuration=None,
        spark_event_logs_s3_uri=None,
    ):
        """Runs a processing job.
        Args:
            submit_app_py (str): Path (local or S3) to Python file to submit to Spark as the primary application
            submit_py_files (list[str]): List of paths (local or S3) to provide for `spark-submit --py-files` option
            submit_jars (list[str]): List of paths (local or S3) to provide for `spark-submit --jars` option
            submit_files (list[str]): List of paths (local or S3) to provide for `spark-submit --files` option
            inputs (list[:class:`~sagemaker.processing.ProcessingInput`]): Input files for
                the processing job. These must be provided as
                :class:`~sagemaker.processing.ProcessingInput` objects (default: None).
            outputs (list[:class:`~sagemaker.processing.ProcessingOutput`]): Outputs for
                the processing job. These can be specified as either path strings or
                :class:`~sagemaker.processing.ProcessingOutput` objects (default: None).
            arguments (list[str]): A list of string arguments to be passed to a
                processing job (default: None).
            wait (bool): Whether the call should wait until the job completes (default: True).
            logs (bool): Whether to show the logs produced by the job.
                Only meaningful when wait is True (default: True).
            job_name (str): Processing job name. If not specified, the processor generates
                a default job name, based on the base job name and current timestamp.
            experiment_config (dict[str, str]): Experiment management configuration.
                Dictionary contains three optional keys:
                'ExperimentName', 'TrialName', and 'TrialComponentDisplayName'.
            configuration (list[dict] or dict): Configuration for Hadoop, Spark, or Hive.
                List or dictionary of EMR-style classifications.
                https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
            spark_event_logs_s3_uri (str): S3 path where spark application events will
                be published to.
        """
        self._current_job_name = self._generate_current_job_name(job_name=job_name)

        if not submit_app_py:
            raise ValueError("submit_app_py is required")

        if submit_py_files:
            py_files_input, py_files_opt = self._stage_submit_deps(
                submit_py_files, _SparkProcessorBase._SUBMIT_PY_FILES_INPUT_CHANNEL_NAME
            )
            if py_files_input:
                inputs = inputs if inputs else []
                inputs.append(py_files_input)
            if py_files_opt:
                self.command.extend([f"--{_SparkProcessorBase._SUBMIT_PY_FILES_INPUT_CHANNEL_NAME}", py_files_opt])

        if submit_jars:
            jars_input, jars_opt = self._stage_submit_deps(
                submit_jars, _SparkProcessorBase._SUBMIT_JARS_INPUT_CHANNEL_NAME
            )
            if jars_input:
                inputs = inputs if inputs else []
                inputs.append(jars_input)
            if jars_opt:
                self.command.extend([f"--{_SparkProcessorBase._SUBMIT_JARS_INPUT_CHANNEL_NAME}", jars_opt])

        if submit_files:
            files_input, files_opt = self._stage_submit_deps(
                submit_files, _SparkProcessorBase._SUBMIT_FILES_INPUT_CHANNEL_NAME
            )
            if files_input:
                inputs = inputs if inputs else []
                inputs.append(files_input)
            if files_opt:
                self.command.extend([f"--{_SparkProcessorBase._SUBMIT_FILES_INPUT_CHANNEL_NAME}", files_opt])

        super().run(
            submit_app=submit_app_py,
            inputs=inputs,
            outputs=outputs,
            arguments=arguments,
            wait=wait,
            logs=logs,
            job_name=job_name,
            experiment_config=experiment_config,
            configuration=configuration,
            spark_event_logs_s3_uri=spark_event_logs_s3_uri,
        )


class SparkJarProcessor(_SparkProcessorBase):
    """Handles Amazon SageMaker processing tasks for jobs using Spark with Java or Scala Jars."""

    def __init__(
        self,
        role,
        instance_type,
        instance_count,
        framework_version=None,
        image_uri=None,
        command=None,
        volume_size_in_gb=30,
        volume_kms_key=None,
        output_kms_key=None,
        max_runtime_in_seconds=None,
        base_job_name=None,
        sagemaker_session=None,
        env=None,
        tags=None,
        network_config=None,
    ):
        """Initialize a ``SparkJarProcessor`` instance. The SparkProcessor
        handles Amazon SageMaker processing tasks for jobs using SageMaker Spark.

        Args:
            framework_version (str): The version of SageMaker PySpark.
            role (str): An AWS IAM role name or ARN. The Amazon SageMaker training jobs
                and APIs that create Amazon SageMaker endpoints use this role
                to access training data and model artifacts. After the endpoint
                is created, the inference code might use the IAM role, if it
                needs to access an AWS resource.
            instance_type (str): Type of EC2 instance to use for
                processing, for example, 'ml.c4.xlarge'.
            instance_count (int): The number of instances to run
                the Processing job with. Defaults to 1.
            command ([str]): The command to run, along with any command-line flags.
                Example: ["python3", "-v"]. If not provided, ["python3"] or ["python2"]
                will be chosen based on the py_version parameter.
            volume_size_in_gb (int): Size in GB of the EBS volume to
                use for storing data during processing (default: 30).
            volume_kms_key (str): A KMS key for the processing
                volume.
            output_kms_key (str): The KMS key id for all ProcessingOutputs.
            max_runtime_in_seconds (int): Timeout in seconds.
                After this amount of time Amazon SageMaker terminates the job
                regardless of its current status.
            base_job_name (str): Prefix for processing name. If not specified,
                the processor generates a default job name, based on the
                training image name and current timestamp.
            sagemaker_session (sagemaker.session.Session): Session object which
                manages interactions with Amazon SageMaker APIs and any other
                AWS services needed. If not specified, the processor creates one
                using the default AWS configuration chain.
            env (dict): Environment variables to be passed to the processing job.
            tags ([dict]): List of tags to be passed to the processing job.
            network_config (sagemaker.network.NetworkConfig): A NetworkConfig
                object that configures network isolation, encryption of
                inter-container traffic, security group IDs, and subnets.
        """

        super(SparkJarProcessor, self).__init__(
            role=role,
            instance_count=instance_count,
            instance_type=instance_type,
            framework_version=framework_version,
            image_uri=image_uri,
            command=command,
            volume_size_in_gb=volume_size_in_gb,
            volume_kms_key=volume_kms_key,
            output_kms_key=output_kms_key,
            max_runtime_in_seconds=max_runtime_in_seconds,
            base_job_name=base_job_name,
            sagemaker_session=sagemaker_session,
            env=env,
            tags=tags,
            network_config=network_config,
        )

    def run(
        self,
        submit_app_jar,
        submit_class,
        submit_jars=[],
        submit_files=[],
        inputs=None,
        outputs=None,
        arguments=None,
        wait=True,
        logs=True,
        job_name=None,
        experiment_config=None,
        configuration=None,
        spark_event_logs_s3_uri=None,
    ):
        """Runs a processing job.
        Args:
            submit_app_jar (str): Path (local or S3) to Jar file to submit to Spark as the primary application
            submit_class (str): Java class reference to submit to Spark as the primary application
            submit_jars (list[str]): List of paths (local or S3) to provide for `spark-submit --jars` option
            submit_files (list[str]): List of paths (local or S3) to provide for `spark-submit --files` option
            inputs (list[:class:`~sagemaker.processing.ProcessingInput`]): Input files for
                the processing job. These must be provided as
                :class:`~sagemaker.processing.ProcessingInput` objects (default: None).
            outputs (list[:class:`~sagemaker.processing.ProcessingOutput`]): Outputs for
                the processing job. These can be specified as either path strings or
                :class:`~sagemaker.processing.ProcessingOutput` objects (default: None).
            arguments (list[str]): A list of string arguments to be passed to a
                processing job (default: None).
            wait (bool): Whether the call should wait until the job completes (default: True).
            logs (bool): Whether to show the logs produced by the job.
                Only meaningful when wait is True (default: True).
            job_name (str): Processing job name. If not specified, the processor generates
                a default job name, based on the base job name and current timestamp.
            experiment_config (dict[str, str]): Experiment management configuration.
                Dictionary contains three optional keys:
                'ExperimentName', 'TrialName', and 'TrialComponentDisplayName'.
            configuration (list[dict] or dict): Configuration for Hadoop, Spark, or Hive.
                List or dictionary of EMR-style classifications.
                https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
            spark_event_logs_s3_uri (str): S3 path where spark application events will
                be published to.
        """
        self._current_job_name = self._generate_current_job_name(job_name=job_name)

        if not submit_app_jar:
            raise ValueError("submit_app_jar is required")

        if submit_class:
            self.command.extend(["--class", submit_class])
        else:
            raise ValueError("submit_class is required")

        if submit_jars:
            jars_input, jars_opt = self._stage_submit_deps(
                submit_jars, _SparkProcessorBase._SUBMIT_JARS_INPUT_CHANNEL_NAME
            )
            if jars_input:
                inputs = inputs if inputs else []
                inputs.append(jars_input)
            if jars_opt:
                self.command.extend([f"--{_SparkProcessorBase._SUBMIT_JARS_INPUT_CHANNEL_NAME}", jars_opt])

        if submit_files:
            files_input, files_opt = self._stage_submit_deps(
                submit_files, _SparkProcessorBase._SUBMIT_FILES_INPUT_CHANNEL_NAME
            )
            if files_input:
                inputs = inputs if inputs else []
                inputs.append(files_input)
            if files_opt:
                self.command.extend([f"--{_SparkProcessorBase._SUBMIT_FILES_INPUT_CHANNEL_NAME}", files_opt])

        super().run(
            submit_app_jar,
            inputs=inputs,
            outputs=outputs,
            arguments=arguments,
            wait=wait,
            logs=logs,
            job_name=self._current_job_name,
            experiment_config=experiment_config,
            configuration=configuration,
            spark_event_logs_s3_uri=spark_event_logs_s3_uri,
        )


class _HistoryServer:
    _CONTAINER_NAME = "history_server"
    _ENTRY_POINT = "smspark-history-server"
    ARG_EVENT_LOGS_S3_URI = "event_logs_s3_uri"
    ARG_REMOTE_DOMAIN_NAME = "remote_domain_name"

    _HISTORY_SERVER_ARGS_FORMAT_MAP = {
        ARG_EVENT_LOGS_S3_URI : "--event-logs-s3-uri {} ",
        ARG_REMOTE_DOMAIN_NAME: "--remote-domain-name {} ",
    }

    def __init__(self, cli_args, image_uri):
        self.cli_args = cli_args
        self.image_uri = image_uri
        self.run_history_server_command = self._get_run_history_server_cmd()

    def run(self):
        self.down()
        print("Starting history server...")
        print(self.run_history_server_command)
        process = subprocess.Popen(self.run_history_server_command, shell=True)
        return process

    def down(self):
        subprocess.call("docker stop " + _HistoryServer._CONTAINER_NAME, shell=True)
        subprocess.call("docker rm " + _HistoryServer._CONTAINER_NAME, shell=True)
        print("History server terminated")

    # This method belongs to _HistoryServer because _CONTAINER_NAME(app name) belongs to _HistoryServer
    # In the future, dynamically creating new app name, available port should also belong to _HistoryServer
    # rather than PySparkProcessor
    def _get_run_history_server_cmd(self):
        env_options = ""
        ser_cli_args = ""
        for key, value in self.cli_args.items():
            if key in self._HISTORY_SERVER_ARGS_FORMAT_MAP:
                ser_cli_args += self._HISTORY_SERVER_ARGS_FORMAT_MAP[key].format(value)
            else:
                env_options += "--env {}={} ".format(key, value)
        print(ser_cli_args)
        cmd = "docker run {} --name {} --network host --entrypoint {} {} {}".format(
            env_options, _HistoryServer._CONTAINER_NAME, _HistoryServer._ENTRY_POINT, self.image_uri, ser_cli_args
        )
        return cmd

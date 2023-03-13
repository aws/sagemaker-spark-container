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
import os
from unittest.mock import patch
from smspark.platform_utils import is_training_job, get_resouce_config_path, get_config_input_path


@patch("os.path.exists", return_value=True)
def test_is_training_job(mock_path_exists) -> None:
    assert is_training_job() is True
    mock_path_exists.assert_called_once_with("/opt/ml/input/config/resourceconfig.json")


@patch("smspark.platform_utils.is_training_job", return_value=True)
def test_get_resource_config_path_for_training(mock_is_training_job) -> None:
    resouce_config_path = get_resouce_config_path()
    assert resouce_config_path == "/opt/ml/input/config/resourceconfig.json"


@patch("smspark.platform_utils.is_training_job", return_value=False)
def test_get_resource_config_path_for_processing(mock_is_training_job) -> None:
    resouce_config_path = get_resouce_config_path()
    assert resouce_config_path == "/opt/ml/config/resourceconfig.json"


@patch("smspark.platform_utils.is_training_job", return_value=True)
def test_get_config_input_path_for_training(mock_is_training_job) -> None:
    resouce_config_path = get_config_input_path()
    assert resouce_config_path == "/opt/ml/input/data/conf/configuration.json"


@patch("smspark.platform_utils.is_training_job", return_value=False)
def test_get_config_input_path_for_processing(mock_is_training_job) -> None:
    resouce_config_path = get_config_input_path()
    assert resouce_config_path == "/opt/ml/processing/input/conf/configuration.json"

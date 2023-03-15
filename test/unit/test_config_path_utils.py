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
from unittest.mock import patch, call
from smspark.config_path_utils import get_config_path, ConfigPathTypes


@patch("os.path.exists", return_value=True)
def test_get_config_path_exists(mock_path_exists) -> None:
    expected_resource_config_path = "/opt/ml/input/config/resourceconfig.json"
    assert get_config_path(ConfigPathTypes.RESOURCE_CONFIG) == expected_resource_config_path
    mock_path_exists.assert_called_once_with(expected_resource_config_path)


@patch("os.path.exists", return_value=False)
def test_get_config_path_non_exists(mock_path_exists) -> None:
    assert get_config_path(ConfigPathTypes.RESOURCE_CONFIG) == None
    mock_path_exists.assert_has_calls(
        [call("/opt/ml/input/config/resourceconfig.json"), call("/opt/ml/config/resourceconfig.json")]
    )

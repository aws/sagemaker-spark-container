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
"""Utils distinguishs between configuration differences between training and processing."""
import os

TRAINING_RESOURCE_CONFIG_PATH = "/opt/ml/input/config/resourceconfig.json"
PROCESSING_RESOUCE_CONFIG_PATH = "/opt/ml/config/resourceconfig.json"

TRAINING_CONFIG_INPUT_PATH = "/opt/ml/input/data/conf/configuration.json"
PROCESSING_CONFIG_INPUT_PATH = "/opt/ml/processing/input/conf/configuration.json"


def is_training_job() -> bool:
    """Decide whether the current job is training."""
    return os.path.exists(TRAINING_RESOURCE_CONFIG_PATH)


def get_resouce_config_path() -> str:
    """Return the resource config path based on the current platform."""
    if is_training_job():
        return TRAINING_RESOURCE_CONFIG_PATH
    else:
        return PROCESSING_RESOUCE_CONFIG_PATH


def get_config_input_path() -> str:
    """Return the input config path based on the current platform."""
    if is_training_job():
        return TRAINING_CONFIG_INPUT_PATH
    else:
        return PROCESSING_CONFIG_INPUT_PATH

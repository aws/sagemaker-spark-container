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
"""Utils to retrieve the file paths for different config types."""
import os
from enum import Enum
from typing import Optional


class ConfigPathTypes(Enum):
    """An enum class which stores all possible values for each of the config type."""

    RESOURCE_CONFIG = ["/opt/ml/input/config/resourceconfig.json", "/opt/ml/config/resourceconfig.json"]
    USER_CONFIGURATION_INPUT = [
        "/opt/ml/input/data/conf/configuration.json",
        "/opt/ml/processing/input/conf/configuration.json",
    ]


def get_config_path(path_types: ConfigPathTypes) -> Optional[str]:
    """Return the config path of corresponding path type."""
    for path in path_types.value:
        if os.path.exists(path):
            return path

    return None

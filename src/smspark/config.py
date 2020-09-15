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
"""Contains configuration classes and serializers for configuration."""
# https://docs.python.org/3/whatsnew/3.7.html#pep-563-postponed-evaluation-of-annotations
from __future__ import annotations

import pathlib
from collections import namedtuple
from dataclasses import dataclass
from typing import ClassVar, Mapping, Sequence

_ClassificationData = namedtuple("ClassificationData", ["classification", "path", "serializer"])

EMR_CONFIGURE_APPS_URL = "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html"


def properties_serializer(configuration: Configuration) -> str:
    """Serialize configuration to .properties files."""
    lines = [f"{key}={val}" for key, val in configuration.Properties.items()]
    return "\n".join(lines) + "\n"


def xml_serializer(configuration: Configuration) -> str:
    """Serialize configuration to properties to insert into .xml files."""
    result = ""
    for name, value in configuration.Properties.items():
        sb = "  <property>\n"
        sb += f"    <name>{name}</name>\n"
        sb += f"    <value>{value}</value>\n"
        sb += "  </property>\n"
        result += sb
    return result


def env_serializer(configuration: Configuration) -> str:
    """Serialize configuration to .env files.

    The inner nested Configuration object contains
    the keys, values, and properties to create lines of env.
    """
    lines = []
    for inner_configuration in configuration.Configurations:
        if inner_configuration.Classification != "export":
            raise ValueError(
                "env classifications must use the 'export' sub-classification. Please refer to {} for more information.".format(
                    EMR_CONFIGURE_APPS_URL
                )
            )
        for key, val in inner_configuration.Properties.items():
            lines.append(f"export {key}={val}")
    return "\n".join(lines) + "\n"


def conf_serializer(configuration: Configuration) -> str:
    """Serialize configuration to .conf files."""
    lines = [f"{key} {val}" for key, val in configuration.Properties.items()]
    return "\n".join(lines) + "\n"


@dataclass
class Configuration:
    """Dataclass representing configuration for Spark, Yarn, Hive, and Hadoop."""

    Classification: str
    Properties: Mapping[str, str]
    Configurations: Sequence[Configuration] = ()
    classification_data: ClassVar = [
        _ClassificationData("core-site", "/usr/lib/hadoop/etc/hadoop/core-site.xml", xml_serializer),
        _ClassificationData("hadoop-env", "/usr/lib/hadoop/etc/hadoop/hadoop-env.sh", env_serializer),
        _ClassificationData("hadoop-log4j", "/usr/lib/hadoop/etc/hadoop/log4j.properties", properties_serializer),
        _ClassificationData("hive-env", "/usr/lib/hive/conf/hive-env.sh", env_serializer),
        _ClassificationData("hive-log4j", "/usr/lib/hive/conf/hive-log4j2.properties", properties_serializer),
        _ClassificationData(
            "hive-exec-log4j", "/usr/lib/hive/conf/hive-exec-log4j2.properties", properties_serializer,
        ),
        _ClassificationData("hive-site", "/usr/lib/hive/conf/hive-site.xml", xml_serializer),
        _ClassificationData("spark-defaults", "/usr/lib/spark/conf/spark-defaults.conf", conf_serializer),
        _ClassificationData("spark-env", "/usr/lib/spark/conf/spark-env.sh", env_serializer),
        _ClassificationData("spark-log4j", "/usr/lib/spark/conf/log4j.properties", properties_serializer),
        _ClassificationData("spark-hive-site", "/usr/lib/spark/conf/hive-site.xml", xml_serializer),
        _ClassificationData("spark-metrics", "/usr/lib/spark/conf/metrics.properties", properties_serializer),
        _ClassificationData("yarn-env", "/usr/lib/hadoop/etc/hadoop/yarn-env.sh", env_serializer),
        _ClassificationData("yarn-site", "/usr/lib/hadoop/etc/hadoop/yarn-site.xml", xml_serializer),
    ]

    def __post_init__(self) -> None:
        """Perform basic validation on values."""
        valid_classifications = [properties[0] for properties in self.classification_data]
        for classification_data in self.classification_data:
            if self.Classification == classification_data.classification:
                self._data: _ClassificationData = classification_data

        # special case for "*-env" classifications, whose inner nested Configurations list use "export"
        if self.Classification not in valid_classifications and self.Classification != "export":
            raise ValueError(
                "Invalid classification: {}. Must be one of {}. Please refer to {} for more information.".format(
                    self.Classification, valid_classifications, EMR_CONFIGURE_APPS_URL
                )
            )
        if "env" in self.Classification and not self.Configurations:
            raise ValueError(
                "'env' classifications require a sub-configuration."
                + " Please refer to {} for more information".format(EMR_CONFIGURE_APPS_URL)
            )

    @property
    def path(self) -> pathlib.Path:
        """Get a path to where the config should be written to and read from."""
        return pathlib.Path(self._data.path)

    @property
    def serialized(self) -> str:
        """Serialize Configuration to string representation for the configuration's filetype."""
        serializer = self._data.serializer
        serialized_conf: str = serializer(self)
        return serialized_conf

    def write_config(self) -> str:
        """Write or update configuration file."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            pass
        if self._data.serializer == xml_serializer:
            contents = ""
            if self.path.exists():
                with open(self.path, "r") as f:
                    contents += f.read()
                    # inserts properties before the closing </configuration> tag.
                    contents = contents.replace("</configuration>", f"\n{self.serialized}</configuration>")
            else:
                contents += """<?xml version="1.0"?>\n"""
                contents += """<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n"""
                contents += "<configuration>\n"
                contents += f"{self.serialized}\n"
                contents += "</configuration>"
            with open(self.path, "w") as f:
                f.write(contents)
        else:
            with open(self.path, "a") as f:
                f.write(self.serialized)
        with open(self.path, "r") as f:
            conf_string = f.read()
            return conf_string

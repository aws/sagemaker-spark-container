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
import glob
import os

from setuptools import find_packages, setup

with open("VERSION", "r") as version_file:
    version = version_file.read()

setup(
    name="smspark",
    description="Library that enables running Spark Processing jobs on Amazon SageMaker",
    version=version,
    python_requires=">3.7.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob.glob("src/smspark/*.py")],
    author="Amazon Web Services",
    url="https://github.com/aws/smspark/",
    license="Apache License 2.0",
    keywords="ML Amazon AWS AI SageMaker Processing Spark",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
    ],
    setup_requires=["setuptools", "wheel"],
    entry_points={
        "console_scripts": [
            "smspark-submit=smspark.cli:submit_main",
            "smspark-history-server=smspark.history_server_cli:run_history_server",
        ]
    },
)

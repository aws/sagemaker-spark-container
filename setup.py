# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
        "Programming Language :: Python :: 3.8",
    ],
    setup_requires=["setuptools", "wheel"],
    # Be frugal when adding dependencies. Prefer Python's standard library.
    install_requires=[
        "tenacity==5.1.4",  # retrying utils
        "psutil==5.7.0",  # inspecting number of cores
        "click==7.1.2",  # parsing command-line options
        "watchdog==0.10.3",  # watching for filesystem events
        "waitress==1.4.4",  # WSGI python server implementation
        "requests==2.24.0",
    ],
    extras_require={
        "test": [
            "black",
            "tox",
            "mypy",
            "flake8",
            "flake8-docstrings",
            "pytest",
            "pytest-cov",
            "pytest-xdist",
            "docker",
            "sagemaker-python-sdk-spark",  # For SageMaker integration tests
            "docker-compose",  # For local integration tests
        ]
    },
    entry_points={
        "console_scripts": [
            "smspark-submit=smspark.cli:submit_main",
            "smspark-history-server=smspark.history_server_cli:run_history_server",
        ]
    },
)

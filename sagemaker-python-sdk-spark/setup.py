from __future__ import absolute_import

from setuptools import find_packages, setup

packages = ["sagemaker", "sagemaker.spark"]

setup(
    name="sagemaker-python-sdk-spark",
    version="0.1",
    packages=packages,
    author="Amazon Web Services",
    description="Extensions to SageMaker Python SDK for Spark",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=["sagemaker>=1.49"],
    python_requires=">=3",
)

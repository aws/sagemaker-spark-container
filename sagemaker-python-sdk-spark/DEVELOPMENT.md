# Development

## Overview

`sagemaker-python-sdk-spark` extends on SageMaker Python SDK to add SparkProcessor and PySparkProcessor.
When we release SageMaker Spark, these classes will 

The integration tests in the repository root use this library.

## Build and test targets

This project uses GNU `Make` to specify build targets.

`make install` builds and installs the directory in your Python environment. This build
target is invoked by the SageMaker Spark container project in the project root to
install this extension library and use it for integration tests on SageMaker.

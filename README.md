# SageMaker Spark Container

## Spark Overview
Apache Spark™ is a unified analytics engine for large-scale data processing. It provides high-level APIs in Scala, Java, Python, and R, and an optimized engine that supports general computation graphs for data analysis. It also supports a rich set of higher-level tools including Spark SQL for SQL and DataFrames, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for stream processing.

## SageMaker Spark Container
The SageMaker Spark Container is a Docker image used to run batch data processing workloads on Amazon SageMaker using the Apache Spark framework. The 
container images in this repository are used to build the pre-built container images that are used when running Spark jobs on Amazon SageMaker using the SageMaker Python SDK. The pre-built images are available in the Amazon Elastic Container Registry (Amazon ECR), and this repository serves as a reference for those wishing to build their own customized Spark containers for use in Amazon SageMaker.

For the list of available Spark images, see [Available SageMaker Spark Images](available_images.md).

## License
This project is licensed under the Apache-2.0 License.


## Usage in the SageMaker Python SDK

The simplest way to get started with the SageMaker Spark Container is to use the pre-built images via the SageMaker Python SDK.

[Amazon SageMaker Processing — sagemaker 2.5.3 documentation](https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_processing.html#amazon-sagemaker-processing)

## Getting Started With Development

To get started building and testing the SageMaker Spark container, you will have to setup a local development environment.

See instructions in [DEVELOPMENT.md](./DEVELOPMENT.md)

## Contributing
To contribute to this project, please read through [CONTRIBUTING.md](./CONTRIBUTING.md)


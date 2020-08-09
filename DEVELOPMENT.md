# Development

This document describes how to set up a development environment for the SageMaker Spark Docker image
and how to build, develop, test, and release it.

## Build and Test dependencies

You'll need to have python, pytest, docker, and docker-compose installed on your machine
and on your $PATH.

To set up your python environment, I recommend creating and activating a virtual environment
using `venv`, which is part of the Python standard library for python3+:


```bash
python3 -m venv .venv
source activate .venv/bin/activate
```
You may want to activate the Python environment in your `.bashrc` or `.zshrc`.

Then install pytest into the virtual environment:

`python -m pip install pytest`

You can install `docker` and `docker-compose` can be installed by installing Docker for Mac from [Docker's website](https://docs.docker.com/docker-for-mac/install/).

`docker` is used to build and run the Spark

### Scala Test dependencies
Compiling Scala test JARs requires SBT installed on your system.

Mac users can easily install using Homebrew:
`brew install sbt`

For more info see https://www.scala-sbt.org/1.x/docs/Setup.html

To compile the Scala test JAR:
`make build-test-scala`

### Java Test dependencies
Compiling Java test JARs requires Maven installed on your system.

Mac users can easily install using Homebrew:
`brew install maven`

For more info see https://maven.apache.org/install.html

To compile the Java test JAR:
`make build-test-java`

## Build, Test, and Release targets.

This repository uses GNU `make` to run build targets specified in `Makefile`.

For example, `make build` builds the SageMaker Spark Docker image.
`make build-tests` compiles Scala and Java tests.
`make test-local` runs local tests with `docker-compose`.
`make test-sagemaker` builds and installs the Python library under `sagemaker-python-sdk-spark`/,
and uses that library to run tests on SageMaker.

Consult `Makefile` for more build targets.


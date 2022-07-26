# Development
This document describes how to set up a development environment for developing, building, and testing the SageMaker Spark Container image.

## Development Environment Setup
You’ll need to have python, pytest, docker, and docker-compose installed on your machine
and on your $PATH.

This repository uses GNU `make` to run build targets specified in `Makefile`. Consult the `Makefile` for the full list of build targets.

### Pulling Down the Code

1. If you do not already have one, create a GitHub account by following the prompts at [Join Github](https://github.com/join).
2. Create a fork of this repository on GitHub. You should end up with a fork at `https://github.com/<username>/sagemaker-spark-container`.
   1. Follow the instructions at [Fork a Repo](https://help.github.com/en/articles/fork-a-repo) to fork a GitHub repository.
3. Clone your fork of the repository: `git clone https://github.com/<username>/sagemaker-spark-container` where `<username>` is your github username.

### Setting Up The Development Environment

1. To set up your python environment, we recommend creating and activating a virtual environment
using `venv`, which is part of the Python standard library for python3+:

```bash
python3 -m venv .venv
source .venv/bin/activate
```
You may want to activate the Python environment in your `.bashrc` or `.zshrc`.

2. Then install pytest into the virtual environment:

`python -m pip install pytest pytest-parallel`

3. Ensure docker is installed (see: [Get Docker | Docker Documentation](https://docs.docker.com/get-docker/))

`docker` will be used to build and test the Spark container locally

4. Ensure you have access to an AWS account i.e. [setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) your environment such that awscli can access your account via either an IAM user or an IAM role. We recommend an IAM role for use with AWS. For the purposes of testing in your personal account, the following managed permissions should suffice:

-- [AmazonSageMakerFullAccess](https://console.aws.amazon.com/iam/home#policies/arn:aws:iam::aws:policy/AmazonSageMakerFullAccess) <br>
-- [AmazonS3FullAccess](https://console.aws.amazon.com/iam/home#policies/arn:aws:iam::aws:policy/AmazonS3FullAccess) <br>
-- [AmazonEC2ContainerRegistryFullAccess](https://console.aws.amazon.com/iam/home#policies/arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess) <br>

5. [Create](https://docs.aws.amazon.com/cli/latest/reference/ecr/create-repository.html) an ECR repository with the name "sagemaker-spark" in the us-west-2 region

6. Setup required environment variables for the container build:
```
export AWS_ACCOUNT_ID=<YOUR_ACCOUNT_ID>
export REGION=us-west-2
export SPARK_REPOSITORY=sagemaker-spark-processing
export SPARK_REPOSITORY_NAME=sagemaker-spark
export VERSION=latest
export SAGEMAKER_ROLE=<YOUR_SAGEMAKER_ROLE>
```

### Building Scala Test Dependencies
Compiling Scala test JARs requires SBT installed on your system.

Mac users can easily install using Homebrew:
`brew install sbt`

For more info see https://www.scala-sbt.org/1.x/docs/Setup.html

To compile the Scala test JAR:
`make build-test-scala`

### Building Java Test Dependencies
Compiling Java test JARs requires Maven installed on your system.

Mac users can easily install using Homebrew:
`brew install maven`

For more info see https://maven.apache.org/install.html

To compile the Java test JAR:
`make build-test-java`

### Building Your Image

1. To build the container image, run the following command:
```
make build
```

Upon successful build, you will see two tags applied to the image. For example:
```
Successfully tagged sagemaker-spark:2.4-cpu-py37-v0.1
Successfully tagged sagemaker-spark:latest
```

2. To verify that the image is available in your local docker repository, run `docker images`. You should see an image with two tags. For example:
```
✗ docker images
REPOSITORY                                                                             TAG                   IMAGE ID            CREATED             SIZE
sagemaker-spark                                                                        2.4-cpu-py37-v0.1     a748a6e042d2        5 minutes ago        3.06GB
sagemaker-spark                                                                        latest                a748a6e042d2        5 minutes ago        3.06GB
```

### Running Local Tests

To run local tests (unit tests and local container tests using docker compose), run the following command:

```
make test-local
```

### Running SageMaker Tests

To run tests against Amazon SageMaker using your newly built container image, first publish the image to your ECR repository.

1. Bootstrap docker credentials for your repository
```
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com
```

2. Tag the latest Spark image
```
docker tag ${SPARK_REPOSITORY}:latest $AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/$SPARK_REPOSITORY:$VERSION
```

3. Push the latest Spark image to your ECR repository
```
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/$SPARK_REPOSITORY:$VERSION
```

5. Run the SageMaker tests
```
make test-sagemaker
```

## Push the code
1. You need to create PR request in order to merge the code. How to create PR request lists here:https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request
2. You need to get Github access of AWS organization. please following here:https://w.amazon.com/?Open_Source/GitHub
3. Get access to permission specific to a team, example is here:https://github.com/orgs/aws/teams/sagemakerwrite/members
4. Ask a person to review the code and merge it in.This repo needs at least one code reviewer. 
5. The code needs to be signed before pushing. More detail about signing is here:https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits.
Remember in your local, you need to set up: git config --global user.signingkey [key id] and also upload public key into your github account.
6. The email you specify when you created public key must match github email in github settings.

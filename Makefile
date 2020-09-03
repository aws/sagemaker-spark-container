#**********************
# Variable definitions
#**********************

# Variable definitions
SHELL          := /bin/sh

# Set variables if testing locally
ifeq ($(IS_RELEASE_BUILD),)
    SPARK_VERSION := 2.4
    PROCESSOR := cpu
    FRAMEWORK_VERSION := py37
    SM_VERSION := 0.1
    USE_CASE := processing
    BUILD_CONTEXT := ./spark/${USE_CASE}/${SPARK_VERSION}/py3
    AWS_PARTITION := aws
    AWS_DOMAIN := amazonaws.com
    export SPARK_ACCOUNT_ID=$(AWS_ACCOUNT_ID)
    export INTEG_TEST_ACCOUNT=$(AWS_ACCOUNT_ID)
    export INTEG_TEST_ROLE=$(SAGEMAKER_ROLE)
    export DEST_REPO=$(SPARK_REPOSITORY)
    export REGION=us-west-2
endif

ROLE := arn:${AWS_PARTITION}:iam::$(INTEG_TEST_ACCOUNT):role/$(INTEG_TEST_ROLE)
IMAGE_URI := $(SPARK_ACCOUNT_ID).dkr.ecr.$(REGION).$(AWS_DOMAIN)/$(DEST_REPO):$(VERSION)

# default target.
all: build test

# Downloads EMR packages. Skips if the tar file containing EMR packages has been made.
download-emr-packages:
	if [[ ! -s ${BUILD_CONTEXT}/emr-spark-packages.tar ]]; then \
		./scripts/spark-emr-install.sh --build-context ${BUILD_CONTEXT} \
	else \
		echo "Skipping EMR package download."; \
	fi

# Builds and moves container python library into the Docker build context
build-container-library:
	python setup.py bdist_wheel;
	cp dist/*.whl ${BUILD_CONTEXT}

install-container-library:
	pip install --upgrade dist/smspark-0.1-py3-none-any.whl
	safety check  # https://github.com/pyupio/safety

build-static-config:
	./scripts/fetch-ec2-instance-type-info.sh --region ${REGION} --use-case ${USE_CASE} --spark-version ${SPARK_VERSION} \
	--processor ${PROCESSOR} --framework-version ${FRAMEWORK_VERSION} --sm-version ${SM_VERSION}

# Builds docker image.
build: download-emr-packages build-container-library build-static-config
	./scripts/build.sh --region ${REGION} --use-case ${USE_CASE} --spark-version ${SPARK_VERSION} \
	--processor ${PROCESSOR} --framework-version ${FRAMEWORK_VERSION} --sm-version ${SM_VERSION}

# Compiles Scala test JAR
# (Requires SBT: `brew install sbt`)
build-test-scala:
	cd test/resources/code/scala/hello-scala-spark; sbt package

# Compiles Java test JAR
# (Requires Maven: `brew install maven`)
build-test-java:
	cd test/resources/code/java/hello-java-spark; mvn package

build-tests: build-test-scala build-test-java

lint:
	black --check ./src
	black --check ./test
	mypy --follow-imports=skip src/smspark   # see mypy.ini for configuration
	flake8 src         # see .flake8 for configuration

test-unit: build-container-library install-container-library
	python -m pytest -s -vv test/unit

# Only runs local tests.
test-local: install-sdk build-tests
	python -m pytest -s -vv test/integration/local --repo=$(DEST_REPO) --tag=$(VERSION) --role=$(ROLE) --durations=0

# Only runs sagemaker tests
# Use pytest-parallel to run tests in parallel - https://pypi.org/project/pytest-parallel/
test-sagemaker: install-sdk build-tests
	# https://github.com/ansible/ansible/issues/32499#issuecomment-341578864
	OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python -m pytest --workers auto -s -vv test/integration/sagemaker --durations=0 \
        --repo=$(DEST_REPO) --tag=$(VERSION) \
        --role $(ROLE) \
        --image_uri $(IMAGE_URI) \
        --region ${REGION} \
        --domain ${AWS_DOMAIN}

# Runs local tests and sagemaker tests.
test-all: test-local test-sagemaker

# Builds and installs sagemaker-python-sdk-spark library, since it's used in sagemaker tests.
install-sdk:
	cd sagemaker-python-sdk-spark; make

clean-sdk:
	cd sagemaker-python-sdk-spark; make clean

# Makes sure docker containers are cleaned
clean:
	docker-compose down || true
	docker kill $$(docker ps -q) || true
	docker rm $$(docker ps -a -q) || true
	docker network rm $$(docker network ls -q) || true
	rm ${BUILD_CONTEXT}/*.whl || true
	rm -rf dist || true
	rm -rf build || true

# Removes compiled Scala SBT artifacts
clean-test-scala:
	cd test/resources/code/scala/hello-scala-spark; sbt clean; rm -r project/ target/ lib_managed/

# Removes compiled Java Maven artifacts
clean-test-java:
	cd test/resources/code/java/hello-java-spark; mvn clean

clean-tests: clean-test-scala clean-test-java

# Removes downloaded EMR packages.
clean-all: clean clean-tests
	rm ${BUILD_CONTEXT}/emr-spark-packages.tar || true
	rm -rf scripts/emr-spark-packages || true

release:
	./scripts/publish.sh --region ${REGION} --use-case ${USE_CASE} --spark-version ${SPARK_VERSION} \
	--processor ${PROCESSOR} --framework-version ${FRAMEWORK_VERSION} --sm-version ${SM_VERSION}

whitelist:
	./scripts/whitelisting/update_repository_policy.sh --region $(REGION) --repository $(DEST_REPO)


# Targets that don't create a file with the same name as the target.
.PHONY: all download-emr-packages build test test-all install-sdk clean clean-all release whitelist build-container-library

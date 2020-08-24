#**********************
# Variable definitions
#**********************

# Variable definitions
SHELL          := /bin/sh

# Set variables if testing locally
# (FIXME): Replace ROLE and IMAGE_URI with the following, once the image is fixed.
#          The image in alpha (552588484154) needs a HADOOP_CONF_DIR fix
# ROLE       := SageMakerPowerUser  # from integ test account 725164585253
#                                   # arn:aws:iam::725164585253:role/SageMakerPowerUser
# IMAGE_URI  := 552588484154.dkr.ecr.us-west-2.amazonaws.com/sagemaker-spark:$(VERSION)
ifeq ($(IS_RELEASE_BUILD),)
    SPARK_VERSION    := 2.4
    USE_CASE         := processing
    FRAMEWORK_VERSION:= py37
    SM_VERSION       := 0.1
    PROCESSOR        := cpu
    BUILD_CONTEXT    := ./spark/${USE_CASE}/${SPARK_VERSION}/py3
    VERSION          := ${SPARK_VERSION}-${PROCESSOR}-${FRAMEWORK_VERSION}-v${SM_VERSION}
    export DEST_REPO=sagemaker-spark-${USE_CASE}
    export REGION=us-west-2
    ROLE             := AmazonSageMaker-ExecutionRole-20200203T115288
    IMAGE_URI        := 038453126632.dkr.ecr.us-west-2.amazonaws.com/${DEST_REPO}:${VERSION}
else
    ROLE       := arn:aws:iam::$(INTEG_TEST_ACCOUNT):role/$(INTEG_TEST_ROLE)
    IMAGE_URI  :=  $(SPARK_ACCOUNT_ID).dkr.ecr.$(REGION).$(AWS_DOMAIN)/$(DEST_REPO):$(VERSION)
endif

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

# Builds docker image.
build: download-emr-packages build-container-library
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
	black --check ./
	mypy src/smspark   # see mypy.ini for configuration
	flake8 src         # see .flake8.ini for configuration
	flake8 test --ignore D100,D103,D104,D400 # ignore flake-docstrings errors in test

test-unit: build-container-library install-container-library
	pytest -s -vv test/unit

# Only runs local tests.
test-local: install-sdk build-tests
	pytest -s -vv test/integration/local --repo=$(DEST_REPO) --tag=$(VERSION) --role=$(ROLE) --durations=0

# Only runs sagemaker tests
# Use pytest-parallel to run tests in parallel - https://pypi.org/project/pytest-parallel/
test-sagemaker: install-sdk build-tests
	# https://github.com/ansible/ansible/issues/32499#issuecomment-341578864
	OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES pytest --workers auto -s -vv test/integration/sagemaker --repo=$(DEST_REPO) --tag=$(VERSION) --durations=0 \
	--role $(ROLE) \
	--image_uri $(IMAGE_URI) \
	--region ${REGION}

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

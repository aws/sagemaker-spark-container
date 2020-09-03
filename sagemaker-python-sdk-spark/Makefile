VERSION := 0.2

all: build test

build:
	python setup.py bdist_wheel
	pip install --upgrade dist/sagemaker_python_sdk_spark-0.1-py2.py3-none-any.whl

test:
	python -m pytest -s tests

clean:
	rm -rf build
	rm -rf dist
	rm -rf sagemaker_python_sdk_spark.egg-info

.PHONY: all build test clean

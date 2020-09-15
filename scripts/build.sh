#!/bin/bash
#
# Build the docker images.

set -euo pipefail

source scripts/shared.sh

parse_std_args "$@"

echo "building image ${version} ... "
docker build \
    -f ${build_context}/docker/Dockerfile.${processor} \
    -t ${repository}:${version} \
    --build-arg REGION=${REGION} \
    -t sagemaker-spark:latest \
    ${build_context}

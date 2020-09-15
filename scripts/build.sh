#!/bin/bash
# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#
# Build the docker images.
#

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

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

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 137112412989.dkr.ecr.us-west-2.amazonaws.com

echo "building image ${version} ... "
echo "building image under ${build_context}/docker/${framework_version} ... "

# Check if running on Mac with ARM architecture
if [[ "$(uname)" == "Darwin" ]] && [[ "$(uname -m)" == "arm64" ]]; then
    echo "Detected Mac with ARM architecture, using buildx for cross-platform build..."
    
    # Create a new builder instance if it doesn't exist
    if ! docker buildx inspect multi-platform-builder &>/dev/null; then
        echo "Creating new buildx builder 'multi-platform-builder'..."
        docker buildx create --name multi-platform-builder --use
    else
        echo "Using existing buildx builder 'multi-platform-builder'..."
        docker buildx use multi-platform-builder
    fi

    # Build for amd64 architecture (x86_64)
    echo "Building for amd64 architecture..."
    docker buildx build \
        --platform linux/amd64 \
        --output type=docker \
        -f ${build_context}/docker/${framework_version}/Dockerfile.${processor} \
        -t ${repository}:${version} \
        --build-arg REGION=${REGION} \
        -t sagemaker-spark-${use_case}:latest \
        ${build_context} 2>&1 | tee "build_log.txt"
else
    # On Linux x86 or other platforms, use the original build command
    echo "Using standard docker build..."
    docker build \
        -f ${build_context}/docker/${framework_version}/Dockerfile.${processor} \
        -t ${repository}:${version} \
        --build-arg REGION=${REGION} \
        -t sagemaker-spark-${use_case}:latest \
        ${build_context} 2>&1 | tee "build_log.txt"
fi
docker logout https://137112412989.dkr.ecr.us-west-2.amazonaws.com

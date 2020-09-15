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
# Publish images to your ECR account.
#

set -euo pipefail

source scripts/shared.sh

parse_std_args "$@"

$(aws ecr get-login --no-include-email --region ${aws_region} --registry-id ${aws_account})
docker tag ${repository}:${version} ${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${repository}:${version}
docker push ${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${repository}:${version}
docker logout https://${aws_account}.dkr.ecr.${aws_region}.amazonaws.com

#!/bin/bash
# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
# Calls EC2 describe-instance-types to form a json file used by the Spark container for choosing Spark config.
# This will fetch more instance types than needed, but that's OK.
#

set -euo pipefail

source scripts/shared.sh

parse_std_args "$@"

dest_path=${build_context}/aws-config
mkdir -p ${dest_path}
dest_path=${dest_path}/ec2-instance-type-info.json

echo "Fetching EC2 instance type info to ${dest_path} ..."
aws ec2 describe-instance-types --region ${aws_region} | \
  jq -c '[.InstanceTypes[] |
  {
    InstanceType: .InstanceType,
    VCpuInfo: .VCpuInfo,
    MemoryInfo: .MemoryInfo,
    GpuInfo: .GpuInfo
  }] |
  sort_by(.InstanceType)' > ${dest_path}

#!/bin/bash
#
# Calls EC2 describe-instance-types to form a json file used by the Spark container for choosing Spark config.
# This will fetch more instance types than needed, but that's OK.

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
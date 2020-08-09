#!/bin/bash
#
# Publish images to your ECR account.

set -euo pipefail

source scripts/shared.sh

parse_std_args "$@"

$(aws ecr get-login --no-include-email --region ${aws_region} --registry-id ${aws_account})
docker tag ${repository}:${version} ${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${repository}:${version}
docker push ${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${repository}:${version}
docker logout https://${aws_account}.dkr.ecr.${aws_region}.amazonaws.com

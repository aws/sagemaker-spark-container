#!/bin/bash
#
# Update repository policy

set -euo pipefail

function parse_std_args() {

    while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -r|--region)
        region="$2"
        shift
        shift
        ;;
        -v|--repository)
        repository="$2"
        shift
        shift
        ;;
        *) # unknown option
        error "unknown option: $1"
        shift
        ;;
    esac
    done

    [[ -z "${region// }" ]] && error 'missing region'
    [[ -z "${repository// }" ]] && error 'missing repository'

    true
}

parse_std_args "$@"

aws ecr --region "$region" set-repository-policy --repository-name "$repository" --policy-text "file://$(PWD)/scripts/whitelisting/repo-policy.json"

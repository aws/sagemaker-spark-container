#!/bin/bash
#
# Utility functions for build/test scripts.

function error() {
    >&2 echo $1
    >&2 echo "usage: $0 [--region <aws-region>] [--spark-version <spark MAJOR.MINOR version>] [--processor <cpu|gpu>]\
     [--use-case <processing|training>] [--framework-version <py3|py37>] [--sm-version <sagemaker MAJOR.MINOR version?]"
    exit 1
}

function get_aws_account() {
    aws --region $1 sts --endpoint-url https://sts.$1.amazonaws.com get-caller-identity --query 'Account' --output text
}

function parse_std_args() {
    while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -r|--region)
        aws_region="$2"
        shift
        shift
        ;;
        --spark-version)
        spark_version="$2"
        shift
        shift
        ;;
        --processor)
        processor="$2"
        shift
        shift
        ;;
        --use-case)
        use_case="$2"
        shift
        shift
        ;;
        --framework-version)
        framework_version="$2"
        shift
        shift
        ;;
        --sm-version)
        sm_version="$2"
        shift
        shift
        ;;
        *) # unknown option
        error "unknown option: $1"
        shift
        ;;
    esac
    done

    [[ -z "${aws_region// }" ]] && error 'missing aws region'
    [[ -z "${spark_version// }" ]] && error 'missing spark minor version'
    [[ -z "${processor// }" ]] && processor='cpu'
    [[ -z "${use_case// }" ]] && use_case='processing'
    [[ -z "${framework_version// }" ]] && framework_version='py37'
    [[ -z "${sm_version// }" ]] && sm_version='0.1'

    [[ -z "${DEST_REPO}" ]] && repository=sagemaker-spark-$use_case || repository="${DEST_REPO}"
    [[ -z "${REGION}" ]] && aws_region='us-west-2' || aws_region="${REGION}"

    if [ $framework_version == 'py37' ]
    then
      # represents the python sub directory
      build_context=./spark/${use_case}/${spark_version}/py3
    else
      build_context=./spark/${use_case}/${spark_version}/${framework_version}
    fi

    aws_account=$(get_aws_account $aws_region)
    # spark tagging scheme :
    # sagemaker-spark-[processing|training]:[spark_version]-[processor]-[python/scala version]-v[sm_major].[sm_minor]
    version=${spark_version}-${processor}-${framework_version}-v${sm_version}

    true
}

function error_build_context() {
    >&2 echo $1
    >&2 echo "usage: $0 [--build-context <Docker build context>]"
    exit 1
}

function parse_build_context() {
    # Docker build context
    if [[ $1 == "--build-context" ]]
    then
        build_context="$2"
    fi

    [[ -z "${build_context// }" ]] && error_build_context "missing path to docker build context"

    true
}
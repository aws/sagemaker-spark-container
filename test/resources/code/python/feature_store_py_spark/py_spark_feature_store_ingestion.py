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
import time
import logging

import botocore
import boto3
import os
from pyspark.sql import SparkSession
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.feature_store.feature_definition import FeatureDefinition, FeatureTypeEnum
from sagemaker import Session, get_execution_role

if __name__ == "__main__":

    region = os.getenv("AWS_REGION")
    boto_session = boto3.Session(region_name=region)
    sagemaker_client = boto_session.client(service_name="sagemaker")
    featurestore_runtime = boto_session.client(service_name="sagemaker-featurestore-runtime")

    feature_store_session = Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        sagemaker_featurestore_runtime_client=featurestore_runtime,
    )

    spark = SparkSession.builder.appName("SparkContainerTestApp").getOrCreate()
    feature_group_name = "spark-processing-feature-store-test"

    record_identifier = "RecordIdentifier"
    event_time = "EventTime"
    columns = [record_identifier, event_time]
    feature_definitions = [
        FeatureDefinition(record_identifier, FeatureTypeEnum.STRING),
        FeatureDefinition(event_time, FeatureTypeEnum.STRING),
    ]
    feature_group = FeatureGroup(
        name=feature_group_name, feature_definitions=feature_definitions, sagemaker_session=feature_store_session
    )
    role_arn = get_execution_role(feature_store_session)

    try:
        # Create a feature group with only online store enabled
        feature_group.create(
            record_identifier_name=record_identifier,
            event_time_feature_name=event_time,
            enable_online_store=True,
            s3_uri=False,
            role_arn=role_arn,
        )
        # Wait long enough till the feature group creation finishes
        time.sleep(20)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ResourceInUse":
            logging.info("Feature group %s is already created, skip the creation.", feature_group_name)
    except Exception as error:
        raise error

    data = [("1", "2021-03-02T12:20:12Z"), ("2", "2021-03-02T12:20:13Z"), ("3", "2021-03-02T12:20:14Z")]

    input_df = spark.createDataFrame(data).toDF(*columns)
    fs_manager = FeatureStoreManager()

    describe_response = feature_group.describe()

    logging.info("Starting input data ingestion to %s ...", feature_group_name)

    fs_manager.ingest_data(input_data_frame=input_df, feature_group_arn=describe_response["FeatureGroupArn"])

    logging.info("Input data ingestion to %s succeeded!", feature_group_name)
    feature_group.delete()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    # Log request context for security investigations
    request_context = event.get("requestContext", {})
    identity = request_context.get("identity", {})
    
    logger.info(json.dumps({
        "event": "request_received",
        "request_id": context.request_id,
        "source_ip": identity.get("sourceIp"),
        "user_agent": identity.get("userAgent"),
        "request_time": request_context.get("requestTime"),
    }))
    
    table = os.environ.get("TABLE_NAME")
    logger.info(json.dumps({"event": "table_loaded", "table_name": table}))
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(json.dumps({"event": "processing_item", "item_id": item.get("id")}))
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            logger.info(json.dumps({"event": "item_inserted", "item_id": id}))
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info(json.dumps({"event": "no_payload_received"}))
            item_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": item_id},
                },
            )
            message = "Successfully inserted data!"
            logger.info(json.dumps({"event": "default_item_inserted", "item_id": item_id}))
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "event": "error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "request_id": context.request_id,
        }))
        raise

import io
import json
import logging
import os
import sys
from typing import Dict, Iterable, Tuple

import boto3
import numpy as np
import torch
import watchtower
from botocore.config import Config
from botocore.exceptions import ClientError
from PIL import Image


# Environment variables
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "uploads")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "processed")
UPLOAD_QUEUE_NAME = os.environ.get("UPLOAD_QUEUE_NAME", "upload-events")
LOG_GROUP_NAME = os.environ.get("CLOUDWATCH_LOG_GROUP", "/local/processor")
LOG_STREAM_NAME = os.environ.get("CLOUDWATCH_LOG_STREAM", "processor")
COST_PER_IMAGE = float(os.environ.get("COST_PER_IMAGE_USD", "0.0005"))


def build_common_kwargs() -> Dict[str, str]:
    """Common configuration for boto3 clients respecting LocalStack endpoints."""

    kwargs: Dict[str, str] = {"region_name": AWS_REGION}
    if AWS_ENDPOINT_URL:
        kwargs["endpoint_url"] = AWS_ENDPOINT_URL
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs.update(
            {
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            }
        )
    return kwargs


COMMON_KWARGS = build_common_kwargs()
S3_CONFIG = Config(signature_version="s3v4", s3={"addressing_style": "path"})

s3 = boto3.client("s3", config=S3_CONFIG, **COMMON_KWARGS)
sqs = boto3.client("sqs", **COMMON_KWARGS)
cloudwatch = boto3.client("cloudwatch", **COMMON_KWARGS)
logs_client = boto3.client("logs", **COMMON_KWARGS)


def setup_logging() -> logging.Logger:
    """Configure application logging to send output to stdout and CloudWatch."""

    logger = logging.getLogger("processor")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(stream_handler)

        try:
            logs_client.create_log_group(logGroupName=LOG_GROUP_NAME)
        except ClientError as exc:  # group may already exist
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code not in {"ResourceAlreadyExistsException"}:
                logger.warning("Failed to create log group: %s", exc)
        except Exception as exc:  # LocalStack may not be ready yet
            logger.warning("CloudWatch logs unavailable: %s", exc)

        try:
            logger.addHandler(
                watchtower.CloudWatchLogHandler(
                    log_group=LOG_GROUP_NAME,
                    stream_name=LOG_STREAM_NAME,
                    create_log_group=False,
                    boto3_client=boto3.client("logs", **COMMON_KWARGS),
                )
            )
        except Exception as exc:
            logger.warning("Falling back to stdout logging only: %s", exc)

    return logger


LOGGER = setup_logging()


def ensure_bucket(name: str) -> None:
    try:
        s3.head_bucket(Bucket=name)
    except ClientError:
        LOGGER.info("Creating bucket %s", name)
        s3.create_bucket(Bucket=name)


def ensure_buckets() -> None:
    for bucket in {UPLOAD_BUCKET, OUTPUT_BUCKET}:
        ensure_bucket(bucket)


def ensure_upload_queue() -> Tuple[str, str]:
    """Create the SQS queue used for S3 notifications."""

    LOGGER.info("Ensuring SQS queue %s", UPLOAD_QUEUE_NAME)
    response = sqs.create_queue(
        QueueName=UPLOAD_QUEUE_NAME,
        Attributes={"ReceiveMessageWaitTimeSeconds": "20"},
    )
    queue_url = response["QueueUrl"]
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3Uploads",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {"aws:SourceArn": f"arn:aws:s3:::{UPLOAD_BUCKET}"}
                },
            }
        ],
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)}
    )
    return queue_url, queue_arn


def configure_upload_bucket_notifications(queue_arn: str) -> None:
    LOGGER.info(
        "Configuring S3 notifications for bucket %s to queue %s",
        UPLOAD_BUCKET,
        queue_arn,
    )
    notification_configuration = {
        "QueueConfigurations": [
            {
                "Id": "UploadEvents",
                "QueueArn": queue_arn,
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=UPLOAD_BUCKET,
        NotificationConfiguration=notification_configuration,
    )


def ensure_infrastructure() -> str:
    ensure_buckets()
    queue_url, queue_arn = ensure_upload_queue()
    configure_upload_bucket_notifications(queue_arn)
    return queue_url


def read_messages(queue_url: str) -> Iterable[Dict[str, str]]:
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=20,
        )
        for message in response.get("Messages", []):
            yield message


def parse_s3_records(message_body: str) -> Iterable[Dict[str, Dict[str, str]]]:
    try:
        payload = json.loads(message_body)
    except json.JSONDecodeError:
        LOGGER.warning("Skipping non-JSON SQS message: %s", message_body)
        return []

    records = payload.get("Records", [])
    if not records and "Message" in payload:
        try:
            nested = json.loads(payload["Message"])
        except json.JSONDecodeError:
            LOGGER.warning("Nested message payload not JSON: %s", payload["Message"])
            return []
        records = nested.get("Records", [])
    return records


def process_image(key: str) -> None:
    LOGGER.info("Processing image %s", key)
    obj = s3.get_object(Bucket=UPLOAD_BUCKET, Key=key)
    img = Image.open(obj["Body"]).convert("RGB")
    arr = np.array(img)
    tensor = torch.tensor(arr, device="cuda")
    inverted = 255 - tensor
    result = Image.fromarray(inverted.to("cpu").numpy().astype("uint8"))

    buf = io.BytesIO()
    result.save(buf, format="JPEG")
    buf.seek(0)
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=key,
        Body=buf,
        ContentType="image/jpeg",
    )

    cloudwatch.put_metric_data(
        Namespace="PhotoPipeline",
        MetricData=[
            {"MetricName": "ImagesProcessed", "Value": 1, "Unit": "Count"},
            {
                "MetricName": "ProcessingCost",
                "Value": COST_PER_IMAGE,
                "Unit": "None",
            },
        ],
    )
    LOGGER.info("Completed processing %s", key)


def consume_events(queue_url: str) -> None:
    for message in read_messages(queue_url):
        message_body = message.get("Body", "")
        records = parse_s3_records(message_body)
        for record in records:
            key = record.get("s3", {}).get("object", {}).get("key")
            if not key:
                continue
            process_image(key)
        receipt_handle = message["ReceiptHandle"]
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def main() -> None:
    queue_url = ensure_infrastructure()
    LOGGER.info("Waiting for upload events...")
    consume_events(queue_url)


if __name__ == "__main__":
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA GPU not available")
    main()

import json
import logging
import os
import queue
import threading
import time
from typing import Dict, Iterable, List

import boto3
import watchtower
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import Flask, Response, jsonify, render_template, request
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "development")

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "uploads")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "processed")
PROCESSED_TOPIC_NAME = os.environ.get("PROCESSED_TOPIC_NAME", "processed-updates")
SNS_HTTP_ENDPOINT = os.environ.get(
    "SNS_HTTP_ENDPOINT", "http://webapp:5000/sns/processed"
)
PUBLIC_S3_ENDPOINT = os.environ.get("PUBLIC_S3_ENDPOINT", "http://localhost:4566")
LOG_GROUP_NAME = os.environ.get("CLOUDWATCH_LOG_GROUP", "/local/webapp")
LOG_STREAM_NAME = os.environ.get("CLOUDWATCH_LOG_STREAM", "webapp")


COMMON_KWARGS: Dict[str, str] = {"region_name": AWS_REGION}
if AWS_ENDPOINT_URL:
    COMMON_KWARGS["endpoint_url"] = AWS_ENDPOINT_URL
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    COMMON_KWARGS.update(
        {
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        }
    )

S3_CLIENT = boto3.client(
    "s3",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    **COMMON_KWARGS,
)
SNS_CLIENT = boto3.client("sns", **COMMON_KWARGS)
LOGS_CLIENT = boto3.client("logs", **COMMON_KWARGS)


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("webapp")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

    try:
        LOGS_CLIENT.create_log_group(logGroupName=LOG_GROUP_NAME)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in {"ResourceAlreadyExistsException"}:
            logger.warning("Unable to create log group: %s", exc)
    except Exception as exc:
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
        logger.warning("Falling back to local logging only: %s", exc)

    return logger


LOGGER = configure_logging()
SUBSCRIBERS: List[queue.Queue] = []
SUBSCRIBERS_LOCK = threading.Lock()
INITIALIZED = threading.Event()
INITIALIZER_STARTED = threading.Event()


def publish_event(payload: Dict[str, str]) -> None:
    LOGGER.info("Publishing update for %s", payload.get("key"))
    with SUBSCRIBERS_LOCK:
        for q in list(SUBSCRIBERS):
            q.put(payload)


def event_stream() -> Iterable[str]:
    client_queue: "queue.Queue[Dict[str, str]]" = queue.Queue()
    with SUBSCRIBERS_LOCK:
        SUBSCRIBERS.append(client_queue)
    try:
        while True:
            message = client_queue.get()
            yield f"data: {json.dumps(message)}\n\n"
    finally:
        with SUBSCRIBERS_LOCK:
            if client_queue in SUBSCRIBERS:
                SUBSCRIBERS.remove(client_queue)


def ensure_bucket(name: str) -> None:
    try:
        S3_CLIENT.head_bucket(Bucket=name)
    except ClientError:
        LOGGER.info("Creating bucket %s", name)
        S3_CLIENT.create_bucket(Bucket=name)


def ensure_topic() -> str:
    response = SNS_CLIENT.create_topic(Name=PROCESSED_TOPIC_NAME)
    topic_arn = response["TopicArn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SNS:Publish",
                "Resource": topic_arn,
                "Condition": {
                    "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{OUTPUT_BUCKET}"}
                },
            }
        ],
    }
    SNS_CLIENT.set_topic_attributes(
        TopicArn=topic_arn, AttributeName="Policy", AttributeValue=json.dumps(policy)
    )
    return topic_arn


def ensure_subscription(topic_arn: str) -> None:
    subscriptions = SNS_CLIENT.list_subscriptions_by_topic(TopicArn=topic_arn)[
        "Subscriptions"
    ]
    endpoints = {sub.get("Endpoint") for sub in subscriptions}
    if SNS_HTTP_ENDPOINT in endpoints:
        return
    SNS_CLIENT.subscribe(
        TopicArn=topic_arn, Protocol="http", Endpoint=SNS_HTTP_ENDPOINT
    )


def ensure_processed_notifications(topic_arn: str) -> None:
    notification_configuration = {
        "TopicConfigurations": [
            {
                "Id": "ProcessedImages",
                "TopicArn": topic_arn,
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }
    S3_CLIENT.put_bucket_notification_configuration(
        Bucket=OUTPUT_BUCKET, NotificationConfiguration=notification_configuration
    )


def ensure_resources() -> None:
    for bucket in {UPLOAD_BUCKET, OUTPUT_BUCKET}:
        ensure_bucket(bucket)
    topic_arn = ensure_topic()
    ensure_subscription(topic_arn)
    ensure_processed_notifications(topic_arn)
    INITIALIZED.set()


def initialize_async() -> None:
    LOGGER.info("Initializing AWS resources for webapp")
    while True:
        try:
            ensure_resources()
            LOGGER.info("Initialization complete")
            return
        except Exception as exc:  # LocalStack may not be ready yet
            LOGGER.warning("Initialization failed, retrying: %s", exc)
            time.sleep(2)


def start_initializer_thread() -> None:
    if INITIALIZER_STARTED.is_set():
        return
    INITIALIZER_STARTED.set()
    threading.Thread(target=initialize_async, daemon=True).start()


if hasattr(app, "before_first_request"):
    app.before_first_request(start_initializer_thread)
else:

    @app.before_request
    def _ensure_initializer_started() -> None:
        start_initializer_thread()


@app.route("/")
def index() -> str:
    images = list_processed_images()
    return render_template("index.html", images=images)


@app.route("/upload", methods=["POST"])
def upload() -> Response:
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file_storage = request.files["file"]
    if not file_storage.filename:
        return jsonify({"error": "Empty filename"}), 400

    filename = secure_filename(file_storage.filename)
    key = f"{int(time.time())}-{filename}"
    try:
        S3_CLIENT.put_object(
            Bucket=UPLOAD_BUCKET,
            Key=key,
            Body=file_storage.read(),
            ContentType=file_storage.mimetype or "application/octet-stream",
        )
    except Exception as exc:
        LOGGER.error("Upload failed: %s", exc)
        return jsonify({"error": "Upload failed"}), 500

    LOGGER.info("Uploaded %s to %s", key, UPLOAD_BUCKET)
    return jsonify({"message": "Uploaded", "key": key}), 201


@app.route("/events")
def events() -> Response:
    def generate() -> Iterable[str]:
        if not INITIALIZED.is_set():
            INITIALIZED.wait()
        yield from event_stream()

    return Response(generate(), mimetype="text/event-stream")


def _normalize_url(url: str) -> str:
    if not AWS_ENDPOINT_URL:
        return url
    return url.replace(AWS_ENDPOINT_URL, PUBLIC_S3_ENDPOINT)


def list_processed_images() -> List[Dict[str, str]]:
    objects: List[Dict[str, str]] = []
    try:
        response = S3_CLIENT.list_objects_v2(Bucket=OUTPUT_BUCKET)
    except ClientError:
        return objects

    for obj in response.get("Contents", []):
        key = obj["Key"]
        presigned = S3_CLIENT.generate_presigned_url(
            "get_object", Params={"Bucket": OUTPUT_BUCKET, "Key": key}, ExpiresIn=3600
        )
        objects.append({"key": key, "url": _normalize_url(presigned)})
    return objects


@app.route("/sns/processed", methods=["POST"])
def sns_processed() -> Response:
    data = request.get_data(as_text=True)
    if not data:
        return ("", 400)

    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        LOGGER.warning("Received non-JSON SNS payload: %s", data)
        return ("", 400)

    message_type = payload.get("Type")
    if message_type == "SubscriptionConfirmation":
        LOGGER.info("Confirming SNS subscription")
        token = payload.get("Token")
        topic_arn = payload.get("TopicArn")
        if token and topic_arn:
            SNS_CLIENT.confirm_subscription(TopicArn=topic_arn, Token=token)
        return ("", 200)

    if message_type == "Notification":
        try:
            message = json.loads(payload.get("Message", "{}"))
        except json.JSONDecodeError:
            LOGGER.warning("Invalid SNS message body: %s", payload.get("Message"))
            return ("", 400)

        for record in message.get("Records", []):
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name")
            key = s3_info.get("object", {}).get("key")
            if bucket != OUTPUT_BUCKET or not key:
                continue
            presigned = S3_CLIENT.generate_presigned_url(
                "get_object",
                Params={"Bucket": OUTPUT_BUCKET, "Key": key},
                ExpiresIn=3600,
            )
            publish_event({"bucket": bucket, "key": key, "url": _normalize_url(presigned)})

        return ("", 200)

    LOGGER.warning("Unhandled SNS message type: %s", message_type)
    return ("", 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)

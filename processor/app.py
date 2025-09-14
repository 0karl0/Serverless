import io
import os
import time

import boto3
import numpy as np
import torch
from PIL import Image

# Environment variables
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
UPLOAD_BUCKET = os.environ.get("UPLOAD_BUCKET", "uploads")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "processed")

# Configure boto3 for either LocalStack or real AWS
common_kwargs = {"region_name": AWS_REGION}
if AWS_ENDPOINT_URL:
    common_kwargs["endpoint_url"] = AWS_ENDPOINT_URL
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    common_kwargs.update(
        {
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        }
    )
s3 = boto3.client("s3", **common_kwargs)
cloudwatch = boto3.client("cloudwatch", **common_kwargs)


def ensure_buckets():
    """Create buckets if they do not exist."""
    for bucket in [UPLOAD_BUCKET, OUTPUT_BUCKET]:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)


def process_image(key: str) -> None:
    """Download an image, invert colors on GPU, and upload result."""
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
        MetricData=[{"MetricName": "ImagesProcessed", "Value": 1, "Unit": "Count"}],
    )


def main():
    ensure_buckets()
    seen = set()
    while True:
        resp = s3.list_objects_v2(Bucket=UPLOAD_BUCKET)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key not in seen:
                print(f"Processing {key}")
                process_image(key)
                seen.add(key)
        time.sleep(5)


if __name__ == "__main__":
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA GPU not available")
    main()

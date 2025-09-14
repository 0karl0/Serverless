# Serverless Photo Processor (Docker Example)

This repository demonstrates a minimal photo-processing pipeline that can be
run locally on a GPU-enabled machine using Docker. The architecture mirrors the
AWS serverless design with the following local components:

- **LocalStack** – runs a local instance of core AWS services (S3, IAM,
  CloudWatch, ECS, EC2, etc.) so you can explore IAM policies, VPCs, and
  track usage metrics without touching a real AWS account.
- **Processor** – a PyTorch-based container that watches the upload bucket,
  performs a simple GPU operation (color inversion), and writes results to the
  processed bucket while reporting metrics to CloudWatch.

The setup can be used for local development and later ported to actual AWS
services such as Amazon S3 and SageMaker Studio.

## Prerequisites

- Docker with the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
- A GPU-capable machine

## Run Locally

```bash
docker compose up --build
```

LocalStack exposes AWS APIs on `http://localhost:4566`. Example commands using
the AWS CLI:

```bash
aws --endpoint-url=http://localhost:4566 s3 ls
aws --endpoint-url=http://localhost:4566 iam list-users
aws --endpoint-url=http://localhost:4566 cloudwatch list-metrics
aws --endpoint-url=http://localhost:4566 ecs list-clusters
aws --endpoint-url=http://localhost:4566 ec2 describe-vpcs
```

Upload images to the `uploads` bucket and watch as processed images appear in
the `processed` bucket. Each processed image emits a `PhotoPipeline/ImagesProcessed`
metric to CloudWatch, allowing usage tracking.

## Porting to AWS

The local components correspond to AWS services:

| Local Component | AWS Equivalent |
| ----------------|---------------|
| LocalStack S3 buckets `uploads` and `processed` | Amazon S3 buckets |
| Processor container | AWS Lambda invoking SageMaker |

### Running in SageMaker Studio

1. Build and push the processor image to Amazon ECR.
2. In SageMaker Studio, create a processing job or Studio image using that
   container.
3. The container uses standard IAM credentials, so CloudWatch logs, IAM role
   permissions, ECS tasks, and VPC configuration can be inspected in the AWS
   console while jobs run.

The same `AWS_ENDPOINT_URL` environment variable can be omitted in Studio so
the container talks to real AWS services.

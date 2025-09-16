# Serverless Photo Processor (Docker Example)

This repository demonstrates an end-to-end photo-processing pipeline that can
be run locally on a GPU-enabled machine using Docker Compose. The architecture
mirrors a typical AWS serverless design with the following components:

- **LocalStack** – runs local versions of S3, CloudWatch (metrics + logs), SNS,
  SQS, IAM, and the Cost Explorer APIs so you can iterate without touching a
  real AWS account while still collecting usage data.
- **Processor** – a PyTorch-based worker that is triggered by native S3 event
  notifications (delivered through SQS), performs a GPU color inversion, and
  stores the result in the processed bucket while sending CloudWatch metrics
  for both throughput and per-image cost.
- **Web App** – a Flask UI that lets you upload photos, receives SNS
  notifications when the processed image lands in S3 (no polling required),
  and streams updates to the browser via Server-Sent Events.

The setup can be used for local development and later ported to actual AWS
services such as Amazon S3 and SageMaker Studio.

## Prerequisites

- Docker with the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
- A GPU-capable machine

## Run Locally

1. Build and launch the stack:

   ```bash
   docker compose pull        # optional, ensures the latest LocalStack image
   docker compose up --build
   ```

2. Export dummy AWS credentials (LocalStack accepts any values) and explore the
   emulated AWS services:

   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   export AWS_DEFAULT_REGION=us-east-1

   aws --endpoint-url=http://localhost:4566 s3 ls
   aws --endpoint-url=http://localhost:4566 sqs list-queues
   aws --endpoint-url=http://localhost:4566 sns list-topics
   aws --endpoint-url=http://localhost:4566 logs describe-log-groups
   aws --endpoint-url=http://localhost:4566 cloudwatch list-metrics --namespace PhotoPipeline
   aws --endpoint-url=http://localhost:4566 ce get-cost-and-usage --time-period Start=$(date +%Y-%m-01),End=$(date -I) --granularity DAILY --metrics BlendedCost
   ```

3. Open the UI at [http://localhost:5000](http://localhost:5000) to upload
   photos. Each upload triggers the processor automatically via S3 notifications,
   and the processed image appears in the browser as soon as S3 publishes the
   SNS event. Application logs are shipped to CloudWatch log groups
   `/local/webapp` and `/local/processor`, while the processor tracks both the
   `ImagesProcessed` and `ProcessingCost` metrics under the `PhotoPipeline`
   namespace.

## Porting to AWS

The local components correspond to AWS services:

| Local Component | AWS Equivalent |
| ----------------|---------------|
| LocalStack S3 buckets `uploads` and `processed` | Amazon S3 buckets |
| S3 → SQS notification | S3 event notification to SQS |
| Processor container | Lambda/SageMaker processing job |
| SNS + web app webhook | S3 → SNS → HTTPS subscriber |
| Server-Sent Events stream | API Gateway WebSocket / AppSync subscription |

### Running in SageMaker Studio

1. Build and push the processor image to Amazon ECR.
2. In SageMaker Studio, create a processing job or Studio image using that
   container.
3. The container uses standard IAM credentials, so CloudWatch logs, IAM role
   permissions, ECS tasks, and VPC configuration can be inspected in the AWS
   console while jobs run.

The same `AWS_ENDPOINT_URL` environment variable can be omitted in Studio so
the container talks to real AWS services.

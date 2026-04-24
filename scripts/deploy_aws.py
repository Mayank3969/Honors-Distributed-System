#!/usr/bin/env python3
"""
AWS DEPLOYMENT SCRIPT
Author: All team members
Description: One-script AWS setup. Run this ONCE to provision all infrastructure.
             Then update .env with the output URLs.

Prerequisites:
  pip install boto3
  aws configure  (set your access key, secret, region)

Run: python scripts/deploy_aws.py
"""

import os
import json
import time
import subprocess
import sys

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Install boto3: pip install boto3")
    sys.exit(1)

REGION        = "ap-south-1"
PROJECT_NAME  = "log-analytics"
S3_BUCKET     = f"{PROJECT_NAME}-results-bucket"
DYNAMODB_TABLE= f"{PROJECT_NAME}-results"
QUEUE_NAME    = f"{PROJECT_NAME}-queue"
DLQ_NAME      = f"{PROJECT_NAME}-dlq"
ECR_REPO      = f"{PROJECT_NAME}-worker"

sqs = boto3.client("sqs",      region_name=REGION)
s3  = boto3.client("s3",       region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ecr = boto3.client("ecr",      region_name=REGION)
iam = boto3.client("iam",      region_name=REGION)
bat = boto3.client("batch",    region_name=REGION)
sts = boto3.client("sts",      region_name=REGION)

outputs = {}


def step(name):
    print(f"\n[DEPLOY] {name}...")


# ── ACCOUNT INFO ─────────────────────────────────────────────────────────────

step("Getting AWS account info")
identity = sts.get_caller_identity()
ACCOUNT_ID = identity["Account"]
print(f"  Account: {ACCOUNT_ID} | Region: {REGION}")


# ── S3 BUCKET ─────────────────────────────────────────────────────────────────

step("Creating S3 bucket")
try:
    s3.create_bucket(
        Bucket=S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    s3.put_bucket_versioning(
        Bucket=S3_BUCKET,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print(f"  Created: s3://{S3_BUCKET}")
except ClientError as e:
    if "BucketAlreadyOwned" in str(e):
        print(f"  Already exists: s3://{S3_BUCKET}")
    else:
        raise
outputs["S3_BUCKET"] = S3_BUCKET


# ── DYNAMODB TABLE ────────────────────────────────────────────────────────────

step("Creating DynamoDB table")
try:
    ddb.create_table(
        TableName=DYNAMODB_TABLE,
        KeySchema=[
            {"AttributeName": "batch_id",     "KeyType": "HASH"},
            {"AttributeName": "processed_at", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "batch_id",     "AttributeType": "S"},
            {"AttributeName": "processed_at", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=DYNAMODB_TABLE)
    print(f"  Created: {DYNAMODB_TABLE}")
except ClientError as e:
    if "ResourceInUseException" in str(e):
        print(f"  Already exists: {DYNAMODB_TABLE}")
    else:
        raise
outputs["DYNAMODB_TABLE"] = DYNAMODB_TABLE


# ── SQS QUEUES ────────────────────────────────────────────────────────────────

step("Creating SQS Dead Letter Queue")
try:
    resp = sqs.create_queue(QueueName=DLQ_NAME)
    dlq_url = resp["QueueUrl"]
    dlq_attr = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])
    dlq_arn  = dlq_attr["Attributes"]["QueueArn"]
    print(f"  DLQ: {dlq_url}")
except ClientError:
    dlq_url = sqs.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
    dlq_attr = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])
    dlq_arn  = dlq_attr["Attributes"]["QueueArn"]

step("Creating SQS Main Queue")
try:
    resp = sqs.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "86400",
            "ReceiveMessageWaitTimeSeconds": "20",
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": "3",
            }),
        }
    )
    queue_url = resp["QueueUrl"]
    print(f"  Queue: {queue_url}")
except ClientError:
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    print(f"  Already exists: {queue_url}")

outputs["SQS_QUEUE_URL"] = queue_url
outputs["DLQ_URL"]       = dlq_url


# ── ECR REPOSITORY ────────────────────────────────────────────────────────────

step("Creating ECR repository for Docker image")
try:
    resp = ecr.create_repository(repositoryName=ECR_REPO)
    ecr_uri = resp["repository"]["repositoryUri"]
    print(f"  ECR: {ecr_uri}")
except ClientError as e:
    if "RepositoryAlreadyExistsException" in str(e):
        resp = ecr.describe_repositories(repositoryNames=[ECR_REPO])
        ecr_uri = resp["repositories"][0]["repositoryUri"]
        print(f"  Already exists: {ecr_uri}")
    else:
        raise
outputs["ECR_URI"] = ecr_uri


# ── WRITE .env FILE ───────────────────────────────────────────────────────────

step("Writing .env file")
env_content = "\n".join([f"{k}={v}" for k, v in outputs.items()])
env_content += f"\nAWS_REGION={REGION}\n"

with open(".env", "w") as f:
    f.write(env_content)
print("  Written to .env")


# ── SUMMARY ───────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("  DEPLOYMENT COMPLETE")
print("="*60)
for k, v in outputs.items():
    print(f"  {k:<20} = {v}")
print()
print("  NEXT STEPS:")
print("  1. Build + push Docker image:")
print(f"     cd batch_worker")
print(f"     docker build -t {ECR_REPO} .")
print(f"     aws ecr get-login-password --region {REGION} | docker login --username AWS --password-stdin {ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com")
print(f"     docker tag {ECR_REPO}:latest {ecr_uri}:latest")
print(f"     docker push {ecr_uri}:latest")
print()
print("  2. Create Batch Compute Environment + Job Queue in AWS Console")
print("  3. Run client: python client/client_sender.py --mode aws")
print("  4. Run dashboard: python dashboard/app.py")
print("="*60 + "\n")

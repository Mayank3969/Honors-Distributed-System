"""
MODULE 3: BATCH WORKER (SERVER)
Author: Person 3
Description: The distributed worker that runs inside Docker on AWS Batch.
             Multiple instances run in PARALLEL processing different batches.
             Performs log analytics: level counts, error patterns, IP stats.
"""

import json
import os
import sys
import time
import datetime
from datetime import timezone
import re
from collections import Counter
from typing import Optional

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

# ─── CONFIG ────────────────────────────────────────────────────────────────
SQS_QUEUE_URL  = os.getenv("SQS_QUEUE_URL", "")
S3_BUCKET      = os.getenv("S3_BUCKET", "log-analytics-results")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "log-analytics-results")
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
WORKER_ID      = os.getenv("WORKER_ID", f"worker-local-{os.getpid()}")


class LogAnalyzer:
    """
    Core analytics engine.
    Runs on each batch independently → true distributed processing.
    """

    def analyze_batch(self, batch: dict) -> dict:
        logs = batch.get("logs", [])
        if not logs:
            return {}

        batch_id  = batch.get("batch_id", "unknown")
        client_id = batch.get("client_id", "unknown")

        print(f"[WORKER {WORKER_ID}] Analyzing {len(logs)} logs from {client_id}")

        level_counts   = Counter()
        service_counts = Counter()
        ip_counts      = Counter()
        error_messages = []
        critical_msgs  = []

        for log in logs:
            level   = log.get("level", "UNKNOWN")
            service = log.get("service", "unknown")
            ip      = log.get("ip_address", "0.0.0.0")
            msg     = log.get("message", "")

            level_counts[level]     += 1
            service_counts[service] += 1
            ip_counts[ip]           += 1

            if level == "ERROR":
                error_messages.append({
                    "timestamp": log.get("timestamp"),
                    "service":   service,
                    "message":   msg,
                    "ip":        ip,
                })

            if level == "CRITICAL":
                critical_msgs.append({
                    "timestamp": log.get("timestamp"),
                    "service":   service,
                    "message":   msg,
                })

        total_processed_logs = len(logs)
        result = {
            "batch_id":        batch_id,
            "client_id":       client_id,
            "worker_id":       WORKER_ID,
            "processed_at":    datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
            "total_logs":      total_processed_logs,
            "level_counts":    dict(level_counts),
            "error_rate_pct":  round((level_counts["ERROR"] / total_processed_logs) * 100, 2) if total_processed_logs > 0 else 0,
            "critical_count":  level_counts["CRITICAL"],
            "top_services":    dict(service_counts.most_common(5)),
            "top_ips":         dict(ip_counts.most_common(10)),
            "error_samples":   error_messages[:5],
            "critical_alerts": critical_msgs,
            "status":          "SUCCESS",
        }

        self._print_summary(result)
        return result

    def _print_summary(self, r: dict):
        print(f"\n{'-'*50}")
        print(f"  BATCH: {r['batch_id']}")
        print(f"  Worker: {r['worker_id']}")
        print(f"  Total Logs: {r['total_logs']}")
        print(f"  Levels: {r['level_counts']}")
        print(f"  Error Rate: {r['error_rate_pct']}%")
        print(f"  Critical Alerts: {r['critical_count']}")
        print(f"  Top Service: {list(r['top_services'].keys())[0] if r['top_services'] else 'N/A'}")
        print(f"{'-'*50}\n")


class BatchWorker:
    """
    Worker loop: polls SQS → analyzes logs → stores results
    Runs inside Docker container on AWS Batch (or locally for testing)
    """

    def __init__(self):
        self.analyzer = LogAnalyzer()
        if AWS_AVAILABLE and SQS_QUEUE_URL:
            self.sqs      = boto3.client("sqs", region_name=AWS_REGION)
            self.s3       = boto3.client("s3",  region_name=AWS_REGION)
            self.dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
            self.table    = self.dynamodb.Table(DYNAMODB_TABLE)
            self.mode     = "aws"
        else:
            self.sqs = self.s3 = self.dynamodb = self.table = None
            self.mode = "local"

    # ── AWS PROCESSING ─────────────────────────────────────────────────────

    def poll_and_process(self, max_batches: int = 10):
        """Main AWS worker loop"""
        print(f"\n[WORKER {WORKER_ID}] Starting in AWS mode...")
        processed = 0

        while processed < max_batches:
            messages = self._receive_messages()
            if not messages:
                print(f"[WORKER {WORKER_ID}] No messages. Waiting...")
                time.sleep(5)
                continue

            for msg in messages:
                try:
                    batch = json.loads(msg["Body"])
                    result = self.analyzer.analyze_batch(batch)
                    self._store_result_s3(result)
                    self._store_result_dynamodb(result)
                    self._delete_message(msg["ReceiptHandle"])
                    processed += 1

                except Exception as e:
                    print(f"[ERROR] Processing failed: {e}")
                    # Message goes back to queue → retried → DLQ after 3 fails

        print(f"[WORKER {WORKER_ID}] Done. Processed {processed} batches.")

    def _receive_messages(self) -> list:
        try:
            resp = self.sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,          # Long polling
                VisibilityTimeout=300,
            )
            return resp.get("Messages", [])
        except ClientError as e:
            print(f"[ERROR] SQS receive failed: {e}")
            return []

    def _delete_message(self, receipt_handle: str):
        try:
            self.sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            print(f"[ERROR] Delete message failed: {e}")

    def _store_result_s3(self, result: dict):
        key = f"results/{result['client_id']}/{result['batch_id']}.json"
        try:
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(result, indent=2),
                ContentType="application/json",
            )
            print(f"[WORKER] Stored to S3: s3://{S3_BUCKET}/{key}")
        except ClientError as e:
            print(f"[ERROR] S3 store failed: {e}")

    def _store_result_dynamodb(self, result: dict):
        try:
            self.table.put_item(Item={
                "batch_id":        result["batch_id"],
                "processed_at":    result["processed_at"],
                "client_id":       result["client_id"],
                "worker_id":       result["worker_id"],
                "total_logs":      result["total_logs"],
                "error_rate_pct":  str(result["error_rate_pct"]),
                "critical_count":  result["critical_count"],
                "level_counts":    json.dumps(result["level_counts"]),
                "top_services":    json.dumps(result["top_services"]),
                "top_ips":         json.dumps(result["top_ips"]),
                "status":          result["status"],
            })
            print(f"[WORKER] Stored to DynamoDB: {result['batch_id']}")
        except ClientError as e:
            print(f"[ERROR] DynamoDB store failed: {e}")

    # ── LOCAL PROCESSING ───────────────────────────────────────────────────

    def process_local_files(self):
        """Process local batch files (no AWS needed)"""
        inbox = "sample_data/outbox"
        outbox = "sample_data/results"
        os.makedirs(outbox, exist_ok=True)

        files = [f for f in os.listdir(inbox) if f.endswith(".json")] if os.path.exists(inbox) else []

        if not files:
            print(f"[WORKER] No files in {inbox}. Run client first.")
            return

        print(f"\n[WORKER {WORKER_ID}] Found {len(files)} batch files. Processing...\n")

        for filename in sorted(files):
            filepath = os.path.join(inbox, filename)
            with open(filepath) as f:
                batch = json.load(f)

            result = self.analyzer.analyze_batch(batch)

            out_path = os.path.join(outbox, filename.replace(".json", "_result.json"))
            with open(out_path, "w") as f:
                json.dump(result, f, indent=2)

            print(f"[WORKER] Result saved -> {out_path}\n")
            time.sleep(0.3)

        print(f"[WORKER {WORKER_ID}] All batches processed.")


if __name__ == "__main__":
    worker = BatchWorker()
    if worker.mode == "aws":
        worker.poll_and_process()
    else:
        worker.process_local_files()

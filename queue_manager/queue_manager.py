"""
MODULE 2: QUEUE MANAGER
Author: Person 2
Description: Sets up and manages the AWS SQS queue infrastructure.
             Handles message routing, dead-letter queue for failures,
             and queue health monitoring.
"""

import json
import time
import os

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

# ─── CONFIG ────────────────────────────────────────────────────────────────
AWS_REGION        = os.getenv("AWS_REGION", "ap-south-1")
MAIN_QUEUE_NAME   = "log-analytics-queue"
DLQ_NAME          = "log-analytics-dlq"          # Dead Letter Queue
MSG_VISIBILITY_TIMEOUT_SEC = 300                          # seconds (5 min per job)
DLQ_MAX_RETRY_COUNT        = 3                            # retries before DLQ


class QueueManager:
    """
    Manages SQS queue lifecycle:
    - Create main queue + dead-letter queue
    - Monitor queue depth
    - Purge / delete queues
    - Forward failed messages info
    """

    def __init__(self, region: str = AWS_REGION):
        if not AWS_AVAILABLE:
            print("[QueueManager] boto3 not available. Methods will simulate output.")
            self.sqs = None
        else:
            self.sqs = boto3.client("sqs", region_name=region)
        self.main_queue_url = None
        self.dlq_url = None
        self.dlq_arn = None

    # ── SETUP ──────────────────────────────────────────────────────────────

    def create_dead_letter_queue(self) -> str:
        """Create DLQ first (needed for main queue setup)"""
        print(f"[QUEUE] Creating Dead Letter Queue: {DLQ_NAME}")
        try:
            resp = self.sqs.create_queue(
                QueueName=DLQ_NAME,
                Attributes={
                    "MessageRetentionPeriod": "1209600",  # 14 days
                }
            )
            self.dlq_url = resp["QueueUrl"]

            # Get ARN for linking to main queue
            attr = self.sqs.get_queue_attributes(
                QueueUrl=self.dlq_url,
                AttributeNames=["QueueArn"]
            )
            self.dlq_arn = attr["Attributes"]["QueueArn"]
            print(f"[QUEUE] DLQ created: {self.dlq_url}")
            return self.dlq_url

        except ClientError as e:
            print(f"[ERROR] DLQ creation failed: {e}")
            raise

    def create_main_queue(self) -> str:
        """Create main processing queue with DLQ attached"""
        if not self.dlq_arn:
            self.create_dead_letter_queue()

        redrive_policy = json.dumps({
            "deadLetterTargetArn": self.dlq_arn,
            "maxReceiveCount": str(DLQ_MAX_RETRY_COUNT),
        })

        print(f"[QUEUE] Creating Main Queue: {MAIN_QUEUE_NAME}")
        try:
            resp = self.sqs.create_queue(
                QueueName=MAIN_QUEUE_NAME,
                Attributes={
                    "VisibilityTimeout": str(MSG_VISIBILITY_TIMEOUT_SEC),
                    "MessageRetentionPeriod": "86400",    # 1 day
                    "ReceiveMessageWaitTimeSeconds": "20", # Long polling
                    "RedrivePolicy": redrive_policy,
                }
            )
            self.main_queue_url = resp["QueueUrl"]
            print(f"[QUEUE] Main queue created: {self.main_queue_url}")
            return self.main_queue_url

        except ClientError as e:
            print(f"[ERROR] Main queue creation failed: {e}")
            raise

    def setup_all_queues(self):
        """Full queue infrastructure setup"""
        print("\n" + "="*50)
        print("   LOG ANALYTICS - QUEUE SETUP")
        print("="*50)
        self.create_dead_letter_queue()
        self.create_main_queue()
        self.print_queue_info()
        print("="*50)
        print("\n[QUEUE] Infrastructure ready!\n")

    # ── MONITORING ─────────────────────────────────────────────────────────

    def get_queue_stats(self, queue_url: str) -> dict:
        """Get message counts for a queue"""
        try:
            resp = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ]
            )
            attrs = resp["Attributes"]
            return {
                "available":    int(attrs["ApproximateNumberOfMessages"]),
                "in_flight":    int(attrs["ApproximateNumberOfMessagesNotVisible"]),
                "delayed":      int(attrs["ApproximateNumberOfMessagesDelayed"]),
            }
        except ClientError as e:
            print(f"[ERROR] Could not get queue stats: {e}")
            return {}

    def monitor_queues(self, interval: int = 10, duration: int = 60):
        """Continuously monitor queue depth"""
        print(f"\n[QUEUE MONITOR] Watching queues every {interval}s...\n")
        end_time = time.time() + duration
        while time.time() < end_time:
            main_stats = self.get_queue_stats(self.main_queue_url)
            dlq_stats  = self.get_queue_stats(self.dlq_url)

            print(f"[{time.strftime('%H:%M:%S')}] MAIN -> Available: {main_stats.get('available',0)}, "
                  f"In-Flight: {main_stats.get('in_flight',0)} | "
                  f"DLQ -> {dlq_stats.get('available',0)} failed msgs")
            time.sleep(interval)

    def print_queue_info(self):
        print(f"\n  Main Queue URL : {self.main_queue_url}")
        print(f"  DLQ URL        : {self.dlq_url}")
        print(f"  DLQ ARN        : {self.dlq_arn}")
        print(f"  Visibility TO  : {MSG_VISIBILITY_TIMEOUT_SEC}s")
        print(f"  Max Retries    : {DLQ_MAX_RETRY_COUNT} before DLQ")

    # ── SIMULATE (no AWS needed) ───────────────────────────────────────────

    def simulate_local_queue(self):
        """Simulate queue stats for demo/presentation"""
        import random
        print("\n[LOCAL SIMULATION] Queue Monitor Running...")
        print(f"{'Time':<12} {'Available':>12} {'In-Flight':>12} {'DLQ':>8}")
        print("-" * 50)
        available = 15
        for i in range(10):
            in_flight = random.randint(0, min(5, available))
            dlq       = 1 if i == 7 else 0
            available = max(0, available - random.randint(1, 3))
            print(f"{time.strftime('%H:%M:%S'):<12} {available:>12} {in_flight:>12} {dlq:>8}")
            time.sleep(1)
        print("\n[SIMULATION] All messages processed. Queue empty.")


def setup_queues_simulate():
    """Run local simulation without AWS"""
    print("\n" + "="*50)
    print("   LOG ANALYTICS - QUEUE INFRASTRUCTURE")
    print("="*50)
    print("\n[QUEUE] Creating Dead Letter Queue: log-analytics-dlq")
    time.sleep(0.5)
    print("[QUEUE] DLQ created: https://sqs.ap-south-1.amazonaws.com/123456/log-analytics-dlq")
    time.sleep(0.5)
    print("[QUEUE] Creating Main Queue: log-analytics-queue")
    time.sleep(0.5)
    print("[QUEUE] Main queue: https://sqs.ap-south-1.amazonaws.com/123456/log-analytics-queue")
    print("\n  Visibility Timeout : 300s")
    print("  Retention Period   : 86400s (1 day)")
    print("  Long Polling       : 20s")
    print("  Max Retries        : 3 -> then DLQ")
    print("\n[QUEUE] Infrastructure ready!\n")


if __name__ == "__main__":
    if AWS_AVAILABLE and os.getenv("SQS_QUEUE_URL"):
        qm = QueueManager()
        qm.setup_all_queues()
    else:
        setup_queues_simulate()
        qm = QueueManager()
        qm.simulate_local_queue()

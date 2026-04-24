"""
MODULE 1: CLIENT - Batch Sender
Author: Person 1
Description: Sends generated log batches to AWS SQS queue.
             Simulates distributed clients submitting work to central queue.
"""

import json
import time
import argparse
import os
import sys

# Ensure log_generator (in same directory) is importable regardless of CWD
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from log_generator import generate_log_batch

# Try importing boto3 (AWS SDK)
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    print("[WARNING] boto3 not installed. Running in LOCAL MODE.")


# ─── CONFIG ────────────────────────────────────────────────────────────────
QUEUE_URL = os.getenv("SQS_QUEUE_URL", "YOUR_SQS_QUEUE_URL_HERE")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BATCH_SIZE = 50       # logs per batch
NUM_BATCHES = 5       # how many batches to send
SEND_DELAY  = 1.0     # seconds between batches


def send_batch_to_sqs(sqs_client, client_id: str, batch_num: int) -> bool:
    """Send one batch of logs to SQS"""
    logs = generate_log_batch(client_id, batch_size=BATCH_SIZE)

    message_body = json.dumps({
        "batch_id": f"{client_id}-batch-{batch_num}",
        "client_id": client_id,
        "batch_num": batch_num,
        "log_count": len(logs),
        "logs": logs,
    })

    try:
        response = sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message_body,
            MessageAttributes={
                "ClientId": {
                    "StringValue": client_id,
                    "DataType": "String"
                },
                "BatchNum": {
                    "StringValue": str(batch_num),
                    "DataType": "Number"
                }
            }
        )
        print(f"[CLIENT {client_id}] Batch {batch_num} sent -> MessageId: {response['MessageId']}")
        return True

    except ClientError as e:
        print(f"[ERROR] Failed to send batch {batch_num}: {e}")
        return False


def run_client_local(client_id: str):
    """Local mode - saves batches to files instead of SQS"""
    print(f"\n[LOCAL MODE] Client {client_id} starting...")
    os.makedirs("sample_data/outbox", exist_ok=True)

    for batch_num in range(1, NUM_BATCHES + 1):
        logs = generate_log_batch(client_id, batch_size=BATCH_SIZE)
        payload = {
            "batch_id": f"{client_id}-batch-{batch_num}",
            "client_id": client_id,
            "batch_num": batch_num,
            "log_count": len(logs),
            "logs": logs,
        }
        filename = f"sample_data/outbox/{client_id}_batch_{batch_num}.json"
        with open(filename, "w") as f:
            json.dump(payload, f, indent=2)

        print(f"[CLIENT {client_id}] Batch {batch_num}/{NUM_BATCHES} -> {filename}")
        time.sleep(SEND_DELAY)

    print(f"[CLIENT {client_id}] Done. Sent {NUM_BATCHES} batches.\n")


def run_client_aws(client_id: str):
    """AWS mode - sends batches to SQS"""
    print(f"\n[AWS MODE] Client {client_id} starting...")
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    success = 0
    for batch_num in range(1, NUM_BATCHES + 1):
        if send_batch_to_sqs(sqs, client_id, batch_num):
            success += 1
        time.sleep(SEND_DELAY)

    print(f"[CLIENT {client_id}] Done. {success}/{NUM_BATCHES} batches sent.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analytics Client")
    parser.add_argument("--client-id", default="client-node-1", help="Unique client ID")
    parser.add_argument("--mode", choices=["local", "aws"], default="local")
    parser.add_argument("--batches", type=int, default=NUM_BATCHES)
    args = parser.parse_args()

    NUM_BATCHES = args.batches

    if args.mode == "aws" and AWS_AVAILABLE:
        run_client_aws(args.client_id)
    else:
        run_client_local(args.client_id)

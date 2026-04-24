"""
MODULE 1: CLIENT - Log Generator
Author: Person 1
Description: Simulates multiple distributed clients generating application logs
             and sending them in batches to the SQS queue.
"""

import random
import time
import datetime
import os

LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
SERVICES = ["auth-service", "payment-service", "user-service", "inventory-service", "notification-service"]
IPS = [f"192.168.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(20)]

MESSAGES = {
    "INFO": [
        "User login successful",
        "Request processed successfully",
        "Cache refreshed",
        "Heartbeat OK",
        "Session started",
        "Data fetched from DB",
        "API call completed in {}ms",
    ],
    "WARNING": [
        "High memory usage: {}%",
        "Slow DB query detected: {}ms",
        "Retry attempt {} of 3",
        "Rate limit approaching",
        "Disk usage above 80%",
    ],
    "ERROR": [
        "Database connection failed",
        "Null pointer exception in module {}",
        "Timeout after {}ms",
        "Authentication failed for user {}",
        "File not found: /var/log/app/{}.log",
    ],
    "DEBUG": [
        "Entering function processRequest()",
        "Variable x = {}",
        "DB query executed in {}ms",
        "Config loaded from environment",
    ],
    "CRITICAL": [
        "Service crashed: {}",
        "Out of memory error",
        "Data corruption detected in table {}",
        "SSL certificate expired",
    ],
}


def generate_log_entry(client_id: str) -> dict:
    level = random.choices(
        LOG_LEVELS,
        weights=[50, 20, 15, 10, 5]
    )[0]

    service = random.choice(SERVICES)
    ip = random.choice(IPS)
    msg_template = random.choice(MESSAGES[level])

    # Fill placeholders
    message = msg_template.format(
        random.randint(10, 9999),
        random.randint(1, 100),
        f"user_{random.randint(1000, 9999)}",
        service
    ) if "{}" in msg_template else msg_template

    return {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "client_id": client_id,
        "service": service,
        "level": level,
        "ip_address": ip,
        "message": message,
        "request_id": f"req-{random.randint(100000, 999999)}",
    }


def generate_log_batch(client_id: str, batch_size: int = 50) -> list:
    """Generate a batch of log entries"""
    return [generate_log_entry(client_id) for _ in range(batch_size)]


def save_logs_to_file(logs: list, filename: str):
    """Save logs to local file (for testing without AWS)"""
    os.makedirs("sample_data", exist_ok=True)
    with open(f"sample_data/{filename}", "w") as f:
        import json
        for log in logs:
            f.write(json.dumps(log) + "\n")
    print(f"[CLIENT] Saved {len(logs)} logs to sample_data/{filename}")


if __name__ == "__main__":
    # Generate sample logs for 3 simulated clients
    for client_num in range(1, 4):
        client_id = f"client-node-{client_num}"
        logs = generate_log_batch(client_id, batch_size=100)
        save_logs_to_file(logs, f"client_{client_num}_logs.jsonl")
        print(f"[CLIENT] Generated 100 logs for {client_id}")
        time.sleep(0.5)

    print("\n[CLIENT] All clients done. Check sample_data/ folder.")

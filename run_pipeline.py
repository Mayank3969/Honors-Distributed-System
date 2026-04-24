#!/usr/bin/env python3
"""
MASTER RUNNER — Runs entire pipeline locally for demo/testing
No AWS required. Simulates all 5 modules end-to-end.

Run: python run_pipeline.py
"""

import subprocess
import sys
import os
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def header(text):
    print(f"\n{'='*55}")
    print(f"  {text}")
    print(f"{'='*55}")


def run_step(label, script_path, args=None):
    header(f"STEP: {label}")
    cmd = [sys.executable, script_path] + (args or [])
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\n[ERROR] {label} failed with code {result.returncode}")
        sys.exit(1)
    time.sleep(0.5)


if __name__ == "__main__":
    print("\n" + "#"*55)
    print("  DISTRIBUTED LOG ANALYTICS PIPELINE")
    print("  Honors Project -- Distributed Systems")
    print("  Running in LOCAL MODE (no AWS needed)")
    print("#"*55)

    # Step 1: Queue Manager (simulate infrastructure setup)
    run_step(
        "MODULE 2 -- Queue Manager: Setting up infrastructure",
        os.path.join(ROOT, "queue_manager/queue_manager.py")
    )

    # Step 2: Client sender generates logs and saves to outbox
    run_step(
        "MODULE 1 -- Client Sender: Client 1 sending batches",
        os.path.join(ROOT, "client/client_sender.py"),
        ["--client-id", "client-node-1", "--mode", "local", "--batches", "3"]
    )
    run_step(
        "MODULE 1 -- Client Sender: Client 2 sending batches",
        os.path.join(ROOT, "client/client_sender.py"),
        ["--client-id", "client-node-2", "--mode", "local", "--batches", "3"]
    )

    # Step 3: Batch worker processes all outbox files
    run_step(
        "MODULE 3 -- Batch Worker: Processing batches",
        os.path.join(ROOT, "batch_worker/worker.py")
    )

    # Step 4: Storage layer query test (local files)
    run_step(
        "MODULE 4 -- Storage Layer: Querying results",
        os.path.join(ROOT, "storage/storage_layer.py")
    )

    # Final summary
    header("PIPELINE COMPLETE - Done!")
    print("  All modules ran successfully!")
    print(f"  Results saved to: sample_data/results/")
    print(f"\n  To view dashboard:")
    print(f"    cd dashboard && python app.py")
    print(f"    Open: http://localhost:5000")
    print()

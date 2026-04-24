"""
MODULE 4: STORAGE LAYER
Author: Person 4
Description: Manages all persistent storage for the pipeline.
             - S3: Raw log archives + processed result files
             - DynamoDB: Queryable results table for dashboard
             - Provides query interface for dashboard module
"""

import json
import os
import time
import datetime
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

# ─── CONFIG ────────────────────────────────────────────────────────────────
AWS_REGION     = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET      = os.getenv("S3_BUCKET", "log-analytics-results")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "log-analytics-results")


class StorageSetup:
    """Sets up AWS storage infrastructure"""

    def __init__(self):
        if AWS_AVAILABLE and os.getenv("SQS_QUEUE_URL"):
            self.s3  = boto3.client("s3",  region_name=AWS_REGION)
            self.ddb = boto3.client("dynamodb", region_name=AWS_REGION)
        else:
            self.s3 = self.ddb = None

    def create_s3_bucket(self):
        """Create S3 bucket with folder structure"""
        print(f"[STORAGE] Creating S3 bucket: {S3_BUCKET}")
        try:
            if AWS_REGION == "us-east-1":
                self.s3.create_bucket(Bucket=S3_BUCKET)
            else:
                self.s3.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
                )

            # Enable versioning
            self.s3.put_bucket_versioning(
                Bucket=S3_BUCKET,
                VersioningConfiguration={"Status": "Enabled"}
            )

            # Create folder structure via empty objects
            for prefix in ["raw-logs/", "results/", "archives/"]:
                self.s3.put_object(Bucket=S3_BUCKET, Key=prefix, Body=b"")

            print(f"[STORAGE] S3 bucket created with folders: raw-logs/, results/, archives/")

        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                print(f"[STORAGE] Bucket already exists. Skipping.")
            else:
                raise

    def create_dynamodb_table(self):
        """Create DynamoDB table for processed results"""
        print(f"[STORAGE] Creating DynamoDB table: {DYNAMODB_TABLE}")
        try:
            self.ddb.create_table(
                TableName=DYNAMODB_TABLE,
                KeySchema=[
                    {"AttributeName": "batch_id",     "KeyType": "HASH"},
                    {"AttributeName": "processed_at", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "batch_id",     "AttributeType": "S"},
                    {"AttributeName": "processed_at", "AttributeType": "S"},
                    {"AttributeName": "client_id",    "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "ClientIdIndex",
                        "KeySchema": [
                            {"AttributeName": "client_id", "KeyType": "HASH"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 5,
                            "WriteCapacityUnits": 5,
                        },
                    }
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
            print(f"[STORAGE] DynamoDB table created with GSI on client_id")

        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceInUseException":
                print(f"[STORAGE] Table already exists. Skipping.")
            else:
                raise

    def setup_all(self):
        print("\n" + "="*50)
        print("   LOG ANALYTICS - STORAGE SETUP")
        print("="*50)
        if self.s3:
            self.create_s3_bucket()
            self.create_dynamodb_table()
        else:
            print("[LOCAL] Simulating storage setup...")
            time.sleep(0.5)
            print(f"[STORAGE] S3 bucket: {S3_BUCKET} -> ready")
            print(f"[STORAGE] DynamoDB table: {DYNAMODB_TABLE} -> ready")
        print("="*50 + "\n")


class StorageQuery:
    """
    Query interface for dashboard.
    Reads from DynamoDB (AWS) or local result files (local mode).
    """

    def __init__(self):
        if AWS_AVAILABLE and os.getenv("SQS_QUEUE_URL"):
            self.dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
            self.table    = self.dynamodb.Table(DYNAMODB_TABLE)
            self.s3       = boto3.client("s3", region_name=AWS_REGION)
            self.mode     = "aws"
        else:
            self.mode = "local"

    def get_all_results(self) -> list:
        """Fetch all processed batch results"""
        if self.mode == "aws":
            return self._scan_dynamodb()
        else:
            return self._read_local_results()

    def get_summary_stats(self) -> dict:
        """Aggregate stats across all batches for dashboard"""
        results = self.get_all_results()
        if not results:
            return self._empty_stats()

        total_logs      = sum(r.get("total_logs", 0) for r in results)
        total_errors    = sum(
            json.loads(r["level_counts"]).get("ERROR", 0)
            if isinstance(r.get("level_counts"), str)
            else r.get("level_counts", {}).get("ERROR", 0)
            for r in results
        )
        total_critical  = sum(r.get("critical_count", 0) for r in results)
        total_batches   = len(results)

        # Aggregate level counts
        agg_levels = {}
        for r in results:
            lc = r.get("level_counts", {})
            if isinstance(lc, str):
                lc = json.loads(lc)
            for k, v in lc.items():
                agg_levels[k] = agg_levels.get(k, 0) + v

        # Aggregate top IPs
        agg_ips = {}
        for r in results:
            ips = r.get("top_ips", {})
            if isinstance(ips, str):
                ips = json.loads(ips)
            for ip, count in ips.items():
                agg_ips[ip] = agg_ips.get(ip, 0) + count

        top_ips_sorted = sorted(agg_ips.items(), key=lambda x: x[1], reverse=True)[:10]

        # Workers used
        workers = list(set(r.get("worker_id", "?") for r in results))

        return {
            "total_logs":       total_logs,
            "total_batches":    total_batches,
            "total_errors":     total_errors,
            "total_critical":   total_critical,
            "error_rate_pct":   round((total_errors / total_logs * 100), 2) if total_logs else 0,
            "level_counts":     agg_levels,
            "top_ips":          dict(top_ips_sorted),
            "workers_used":     workers,
            "recent_batches":   results[-5:],
        }

    def _scan_dynamodb(self) -> list:
        try:
            resp = self.table.scan()
            return resp.get("Items", [])
        except Exception as e:
            print(f"[STORAGE] DynamoDB scan failed: {e}")
            return []

    def _read_local_results(self) -> list:
        results = []
        # Resolve path relative to this file's location (project root), not CWD
        base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
        result_dir = base_dir / "sample_data" / "results"
        if not result_dir.exists():
            return []
        for f in sorted(result_dir.glob("*.json")):
            with open(f) as fp:
                results.append(json.load(fp))
        return results

    def _empty_stats(self) -> dict:
        return {
            "total_logs": 0, "total_batches": 0,
            "total_errors": 0, "total_critical": 0,
            "error_rate_pct": 0, "level_counts": {},
            "top_ips": {}, "workers_used": [], "recent_batches": [],
        }


if __name__ == "__main__":
    # Setup storage
    setup = StorageSetup()
    setup.setup_all()

    # Test query
    print("[STORAGE] Testing query interface...")
    query = StorageQuery()
    stats = query.get_summary_stats()
    print(f"[STORAGE] Summary: {json.dumps(stats, indent=2)}")

# Distributed Log Analytics Pipeline
### Honors Project — Distributed Systems | Ramdeobaba University

---

## Project Overview

A fully distributed batch processing system that collects logs from multiple client nodes, queues them via AWS SQS, processes them in parallel using AWS Batch workers (Docker containers), stores results in S3 + DynamoDB, and displays real-time analytics on a web dashboard.

---

## Architecture

```
[Client Node 1] ──┐
[Client Node 2] ──┼──→ [AWS SQS Queue] ──→ [AWS Batch Worker 1] ──┐
[Client Node 3] ──┘          │              [AWS Batch Worker 2] ──┼──→ [S3 + DynamoDB]
                        (Dead Letter Q)     [AWS Batch Worker N] ──┘         │
                                                                        [Dashboard]
```

---

## Team Modules

| Person | File | Role |
|--------|------|------|
| P1 | `client/log_generator.py` + `client_sender.py` | Log generation + batch submission |
| P2 | `queue_manager/queue_manager.py` | SQS setup, DLQ, monitoring |
| P3 | `batch_worker/worker.py` + `Dockerfile` | Distributed log processing |
| P4 | `storage/storage_layer.py` | S3 + DynamoDB setup and queries |
| P5 | `dashboard/app.py` + `templates/` | Flask dashboard + charts |

---

## Running Locally (No AWS Needed)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run full pipeline
python run_pipeline.py

# 3. Start dashboard
cd dashboard
python app.py

# 4. Open browser
# http://localhost:5000
```

---

## AWS Deployment

### Step 1 — Prerequisites
```bash
pip install boto3 awscli
aws configure   # enter your Access Key, Secret Key, Region: ap-south-1
```

### Step 2 — Deploy Infrastructure
```bash
python scripts/deploy_aws.py
# Creates: S3 bucket, DynamoDB table, SQS queue, DLQ, ECR repo
# Writes: .env file with all URLs
```

### Step 3 — Build & Push Docker Image
```bash
cd batch_worker
docker build -t log-analytics-worker .

# Get ECR login (copy from deploy output)
aws ecr get-login-password --region ap-south-1 | \
  docker login --username AWS --password-stdin <ECR_URI>

docker tag log-analytics-worker:latest <ECR_URI>:latest
docker push <ECR_URI>:latest
```

### Step 4 — Create AWS Batch Resources (AWS Console)
1. **Compute Environment**
   - Go to AWS Batch → Compute Environments → Create
   - Type: Managed | Platform: Fargate
   - Name: `log-analytics-compute`

2. **Job Queue**
   - Batch → Job Queues → Create
   - Name: `log-analytics-job-queue`
   - Connect to compute environment above

3. **Job Definition**
   - Batch → Job Definitions → Create
   - Type: Single-node | Platform: Fargate
   - Image: `<your ECR URI>:latest`
   - vCPUs: 0.25 | Memory: 512 MB
   - Environment variables:
     - `SQS_QUEUE_URL` = (from .env)
     - `S3_BUCKET` = `log-analytics-results-bucket`
     - `DYNAMODB_TABLE` = `log-analytics-results`
     - `AWS_REGION` = `ap-south-1`

### Step 5 — Run the Pipeline
```bash
# Terminal 1: Send logs from Client 1
python client/client_sender.py --client-id client-node-1 --mode aws --batches 5

# Terminal 2: Send logs from Client 2 (simulates distributed clients)
python client/client_sender.py --client-id client-node-2 --mode aws --batches 5

# Terminal 3: Monitor queue
python queue_manager/queue_manager.py

# AWS Batch console: Submit job definition manually or via Lambda trigger

# Terminal 4: Start dashboard
cd dashboard && python app.py
```

---

## Key Distributed Systems Concepts Demonstrated

| Concept | How |
|---------|-----|
| **Client-Server** | Python clients → AWS Batch server workers |
| **Batch Processing** | Logs grouped into batches, processed async |
| **Distributed Workers** | Multiple Batch containers run in parallel |
| **Message Queue** | SQS decouples client from server |
| **Fault Tolerance** | Dead Letter Queue handles failures; auto-retry x3 |
| **Scalability** | Batch auto-scales workers based on queue depth |
| **Persistence** | S3 (files) + DynamoDB (queryable results) |
| **Monitoring** | Dashboard + CloudWatch integration |

---

## Project Structure

```
log-analytics/
├── client/
│   ├── log_generator.py       # P1: Generates fake log data
│   └── client_sender.py       # P1: Sends batches to SQS
├── queue_manager/
│   └── queue_manager.py       # P2: SQS setup and monitoring
├── batch_worker/
│   ├── worker.py              # P3: Core processing logic
│   ├── Dockerfile             # P3: Container definition
│   └── requirements.txt
├── storage/
│   └── storage_layer.py       # P4: S3 + DynamoDB layer
├── dashboard/
│   ├── app.py                 # P5: Flask server
│   └── templates/
│       └── dashboard.html     # P5: UI with charts
├── scripts/
│   └── deploy_aws.py          # Full AWS deployment
├── sample_data/               # Auto-generated test data
├── requirements.txt
├── run_pipeline.py            # Local demo runner
└── README.md
```

---

## Viva Prep — Expected Questions

**Q: Why SQS instead of direct client → server?**
A: Decoupling. If server is busy/down, messages wait in queue. Client doesn't need to know server address.

**Q: How is this distributed?**
A: Multiple client nodes send concurrently. Multiple Batch workers process different batches in parallel. No shared state between workers.

**Q: What happens if a worker crashes?**
A: SQS message becomes visible again after VisibilityTimeout (300s). Another worker picks it up. After 3 failures → Dead Letter Queue.

**Q: How does it scale?**
A: AWS Batch auto-provisions more Fargate containers when queue depth increases. No manual intervention.

**Q: What's the role of S3 vs DynamoDB?**
A: S3 stores full JSON result files (raw archive). DynamoDB stores structured summaries for fast queries by the dashboard.

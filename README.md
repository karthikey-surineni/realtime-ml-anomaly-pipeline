# Real-Time ML Anomaly Detection Pipeline

A proof-of-concept real-time ML pipeline that ingests live cryptocurrency market data from Binance, performs streaming feature engineering, and runs IsolationForest inference to detect anomalous volume spikes and price drops.

Built to demonstrate the **same architectural pattern** used in production player churn prediction, fraud detection, and retention trigger systems.

## Quick Start

```bash
uv sync --dev
uv run anomaly-pipeline
```

The pipeline will: fetch 1,000 historical klines → train IsolationForest → connect to Binance WebSocket → score each closed candle → log JSON alerts → persist anomalies to SQLite.

## Local Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BOOTSTRAP PHASE                          │
│                                                                 │
│  Binance REST API ──→ fetch 1,000 klines ──→ RollingBuffer     │
│  (binance_rest.py)     (aiohttp)              (Polars DataFrame)│
│                                                    │            │
│                                          IsolationForest.fit()  │
│                                           (detector.py)         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STREAMING PHASE                            │
│                                                                 │
│  Binance WebSocket ──→ Pydantic ──→ RollingBuffer ──→ Feature  │
│  (binance_ws.py)       Validation   .append()         Engine   │
│  wss://stream...       (schemas.py)  (buffer.py)    (features.py)│
│                                                        │        │
│                                              IsolationForest    │
│                                               .predict()        │
│                                              (detector.py)      │
│                                                   │             │
│                                          ┌────────┴────────┐   │
│                                          │                 │    │
│                                     Normal            Anomaly   │
│                                     (log)          ┌────┴────┐  │
│                                               structlog   SQLite│
│                                               CRITICAL    write │
│                                               alert    (aiosqlite)│
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
src/anomaly_pipeline/
├── main.py              # Async event loop: bootstrap → train → stream → detect
├── config.py            # pydantic-settings (all env vars from .env)
├── schemas.py           # Pydantic V2: BinanceKlineEvent, Kline, AnomalyFeatures, AnomalyEvent
├── logger.py            # structlog JSON configuration
├── connections/
│   ├── binance_ws.py    # WebSocket client with exponential backoff reconnection
│   └── binance_rest.py  # REST client for historical kline bootstrap
├── core/
│   ├── buffer.py        # Polars DataFrame rolling window (mean/std)
│   └── features.py      # Feature engineering (price ROC, volume z-score, spread)
├── ml/
│   └── detector.py      # IsolationForest train/predict wrapper
└── storage/
    └── sqlite_writer.py # Async SQLite anomaly persistence
```

---

## AWS Production Architecture

How each local component maps to a production deployment on AWS:

```
┌──────────────┐     ┌──────────────────┐     ┌───────────────────────┐
│ Event Source  │     │ Stream Ingestion │     │  Feature Store        │
│              │     │                  │     │                       │
│ Kafka / MSK  │────▶│ Kinesis Data     │────▶│ SageMaker Feature     │
│ (player      │     │ Streams          │     │ Store                 │
│  events)     │     │                  │     │ (offline + online)    │
└──────────────┘     └──────────────────┘     └───────────┬───────────┘
                                                          │
                              ┌────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ML INFERENCE LAYER                                │
│                                                                     │
│  ┌─────────────────────┐    ┌──────────────────────────────────┐   │
│  │ Batch Training       │    │ Real-Time Scoring                │   │
│  │                     │    │                                  │   │
│  │ SageMaker Training  │    │ SageMaker Endpoint               │   │
│  │ Job (daily/weekly)  │───▶│ (or Lambda + model artifact)     │   │
│  │                     │    │                                  │   │
│  │ S3 Model Registry   │    │ Scores each event in <100ms      │   │
│  └─────────────────────┘    └──────────────┬───────────────────┘   │
│                                             │                       │
└─────────────────────────────────────────────┼───────────────────────┘
                                              │
                              ┌────────────────┴────────────────┐
                              ▼                                 ▼
                ┌──────────────────────┐          ┌──────────────────────┐
                │ Score Store          │          │ Action Layer         │
                │                      │          │                      │
                │ DynamoDB             │          │ EventBridge Rule     │
                │ (player_id → score,  │          │  → SNS/SQS          │
                │  features, timestamp)│          │  → Lambda            │
                │                      │          │  → Campaign API      │
                │ TTL: 24h             │          │  (Braze, Iterable)   │
                └──────────────────────┘          └──────────────────────┘
```

### Component Mapping

| Local (This PoC) | AWS Production | Interview Talking Point |
|---|---|---|
| Binance WebSocket | **Amazon MSK / Kinesis Data Streams** | "Player events (bets, logins, deposits) flow through Kafka topics partitioned by player_id" |
| `binance_rest.py` (bootstrap) | **S3 Data Lake + Glue Catalog** | "Historical player activity stored in S3 Bronze layer, catalogued by Glue for batch feature computation" |
| `buffer.py` (Polars rolling window) | **SageMaker Feature Store** (online + offline) | "Online store for low-latency feature retrieval during scoring; offline store for batch training on historical features" |
| `features.py` (price ROC, z-score) | **Kinesis Data Analytics / Flink** | "Streaming feature engineering: session frequency, deposit velocity, bet variance — computed as events arrive" |
| `detector.py` (IsolationForest) | **SageMaker Endpoint** | "Model trained on batch features, deployed as real-time endpoint; retrained weekly on new data" |
| `sqlite_writer.py` | **DynamoDB** (score store) | "Player churn scores written to DynamoDB with TTL, queryable by retention team dashboards" |
| structlog CRITICAL alert | **EventBridge → SNS → Lambda** | "When churn score exceeds threshold, EventBridge rule triggers Lambda that calls the campaign API (Braze/Iterable) for personalised retention offers" |
| `.env` config | **AWS Systems Manager Parameter Store** | "All config (model thresholds, feature windows, endpoints) in SSM, no hardcoded values" |

---

## Player Churn Domain Mapping

This PoC uses Binance market data as a **proxy for player activity**. The architectural pattern is identical — only the feature semantics change:

| Anomaly Detection Feature | Player Churn Equivalent | How to Explain |
|---|---|---|
| **Volume** (trade volume per candle) | **Session activity** (bets placed, pages viewed) | "Declining volume over the rolling window signals disengagement — same as a player's session count dropping" |
| **Volume z-score** | **Activity z-score** | "How far the player's current activity deviates from their personal rolling average — a z-score below -2 is a churn signal" |
| **Price ROC** (rate of change) | **Deposit/withdrawal velocity** | "Rapid change in financial behaviour — large withdrawals or deposit halts signal intent to leave" |
| **Spread** (high - low) | **Session variance** | "Erratic behaviour patterns — long sessions then nothing — indicates instability before churn" |
| **Number of trades** (`n` field) | **Actions per session** | "Declining in-session actions even when the player logs in — passive browsing vs active betting" |
| **IsolationForest anomaly** | **Churn risk flag** | "In production, swap for XGBoost/LightGBM supervised model trained on labelled churn outcomes" |
| **Anomaly alert** | **Retention campaign trigger** | "Score crosses threshold → EventBridge → Lambda → personalised push notification or bonus offer" |

---

## Kappa vs Lambda Architecture

A key architectural trade-off for any real-time ML system:

### Lambda Architecture (Batch + Speed layers)
```
                    ┌─── Batch Layer (S3 + Glue + Athena) ───── Serving Layer
Event Source ──────┤                                              (merged view)
                    └─── Speed Layer (Kinesis + Flink) ──────────
```
- **Pros**: Batch layer reprocesses for correctness; speed layer provides low-latency approximate results
- **Cons**: Two codepaths to maintain; eventual consistency between layers; operational complexity
- **When to use**: When batch accuracy matters (financial reporting, compliance audits) AND real-time speed is needed

### Kappa Architecture (Stream-only)
```
Event Source ──── Stream Layer (Kinesis/Kafka + Flink) ──── Serving Layer
                         │
                    Replay from offset
                    (for reprocessing)
```
- **Pros**: Single codebase; simpler operations; replay from stream offset for reprocessing
- **Cons**: Requires durable, replayable stream (Kafka with long retention); harder for complex batch aggregations
- **When to use**: When the stream is the source of truth and reprocessing can be done by replaying

### Recommended Approach for Churn Prediction

> "For a churn prediction pipeline, I'd recommend a **Kappa architecture** with a caveat. Player events flow through MSK (Kafka) — this is the single source of truth. Flink computes streaming features (session counts, deposit velocity) and writes to SageMaker Feature Store's online store. For batch retraining, we replay from Kafka or read from the Feature Store's offline store (backed by S3). This gives us one codebase for feature engineering. The exception is batch ingestion from third-party providers — that's inherently batch and should use a traditional ETL path (Glue + S3 + Athena), not forced through a stream."

---

## Design Discussion: Player Churn Prediction & Retention

### 1. Feature Engineering Pipeline
> "I built a rolling window feature engine using Polars that computes rate-of-change, z-scores, and spread in real-time. In production, this maps to Flink on Kinesis computing streaming features per player — session frequency over 7/14/30 day windows, deposit velocity, bet-to-session ratio. These write to SageMaker Feature Store's online store for real-time scoring and offline store for batch training."

### 2. ML Pipeline (Churn Probability Score)
> "The PoC uses IsolationForest for unsupervised anomaly detection. For churn prediction, I'd use a supervised model — XGBoost or LightGBM — trained on labelled historical data (players who churned vs retained). Training runs as a SageMaker Training Job on a weekly schedule via Step Functions. The model artifact goes to S3 Model Registry, and the SageMaker Endpoint auto-deploys the latest version. Each player gets scored when new events arrive — the endpoint returns a churn probability between 0 and 1."

### 3. Automated Retention Triggers
> "When a player's churn score exceeds a configurable threshold (e.g., 0.75), the scoring Lambda publishes to EventBridge. An EventBridge rule routes to an SQS queue consumed by a retention Lambda that calls the campaign API (Braze/Iterable) with the player's profile and risk tier. Tier 1 (score > 0.9) gets a personalised bonus offer. Tier 2 (0.75-0.9) gets a re-engagement email. All trigger events are logged to S3 for the feedback loop — did the intervention prevent churn? This labelled data feeds back into the next training cycle."

### 4. Data Governance (Bonus Points)
> "Player data is PII-sensitive. Feature Store handles access control via IAM policies. Raw events in S3 are encrypted with KMS. DynamoDB TTL ensures scores are purged after 24 hours. CloudTrail logs all data access. For GDPR/right-to-delete, a Lambda triggered by the compliance API removes all player data from Feature Store, DynamoDB, and S3."

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.13 |
| Package Manager | uv |
| Concurrency | asyncio |
| Stream Client | websockets |
| REST Client | aiohttp |
| Data Processing | Polars |
| ML Model | scikit-learn (IsolationForest) |
| Validation | Pydantic V2, pydantic-settings |
| Logging | structlog (JSON) |
| Storage | SQLite (aiosqlite) |
| Linting | ruff |

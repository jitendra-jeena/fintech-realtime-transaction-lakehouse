# FinTech Real-Time Transaction Lakehouse

**Engineer:** Jitendra Jeena

## Project Overview

A production-style, distributed data platform simulating the backend of a high-volume fintech payment system.

The goal of this project is to demonstrate how modern data engineering architectures handle millions of financial transactions using event streaming, distributed storage, and lakehouse technologies.

The platform processes transaction events through two different paths:

- **Real-time processing** for fraud detection and low-latency transaction lookups
- **Batch processing** for financial reconciliation and analytics

## Architecture

The project follows a Lambda Architecture pattern using:

- Apache Kafka for event streaming
- PySpark Structured Streaming for real-time processing
- Apache Cassandra for low-latency operational queries
- Delta Lake for analytical workloads

```
Transaction Producer
        |
        v
   Apache Kafka
        |
        v
PySpark Structured Streaming
        |
   +----+----+
   |         |
   v         v
Cassandra  Delta Lake
 Hot Path  Cold Path
   |         |
   v         v
Fraud     Analytics
Checks    Reports
```

---

# Development Phases

## Phase 1: GitHub Profile Optimization

Completed.

Profile improvements:

- Created a professional GitHub profile README.
- Highlighted real engineering experience instead of generic technology lists.
- Focused on:
  - Legacy modernization
  - Performance optimization
  - Data platform engineering
  - Telecom and distributed systems experience

Key achievements highlighted:

- Migration of legacy Java batch workloads to Python with improved execution performance.
- Cassandra query optimization using Delta Lake and Spark SQL.
- Experience building scalable batch and streaming pipelines.

---

# Phase 2: Repository Architecture

Completed.

The repository follows a production-style structure:

```
fintech-realtime-transaction-lakehouse/

├── docker/
│   └── Docker Compose configurations

├── src/
│   ├── generators/
│   │   └── Transaction data producer

│   ├── streaming/
│   │   └── PySpark streaming applications

│   ├── batch/
│   │   └── Batch analytics jobs

│   ├── schemas/
│   │   └── Data contracts

│   └── utils/
│       └── Common utilities

├── tests/
│   └── Unit and integration tests

├── scripts/
│   └── Environment setup scripts

├── docs/
│   └── Architecture documentation

└── README.md
```

Repository standards:

- Defined `.gitignore` rules to prevent committing generated data and temporary files.
- Added documentation explaining architecture decisions and technology trade-offs.

---

# Phase 3: Cloud Development Environment

Completed.

Development environment:

- GitHub Codespaces

Reason:

- Provides a cloud-based Linux development environment.
- Avoids local machine virtualization restrictions.
- Provides seamless VS Code integration.

---

# Phase 4: Infrastructure Setup

Completed.

Infrastructure is provisioned using Docker Compose.

Services:

| Service | Purpose |
|---|---|
| Zookeeper | Kafka coordination |
| Apache Kafka | Transaction event streaming |
| Apache Cassandra | Real-time transaction storage |

Configured features:

- Container networking
- Health checks
- Service dependencies

---

# Phase 5: Transaction Data Producer

Completed.

File:

```
src/generators/transaction_producer.py
```

Purpose:

Generates realistic synthetic payment transactions and publishes them to Kafka.

Features:

- UUID-based transaction identifiers
- Multiple currencies
- Transaction success/failure simulation
- JSON message format
- Kafka producer optimization
- Graceful shutdown handling

Kafka topic:

```
payment_transactions
```

---

# Phase 6: PySpark Streaming Consumer

Next Implementation Phase.

File:

```
src/streaming/spark_consumer.py
```

Objective:

Build a PySpark Structured Streaming application that consumes transaction events from Kafka.

Processing flow:

```
Kafka Topic
     |
     v
PySpark Structured Streaming
     |
 +---+---+
 |       |
 v       v
Cassandra Delta Lake
```

Responsibilities:

## Hot Path

Write processed transactions to Cassandra for:

- Customer transaction history
- Fraud detection queries
- Low-latency lookups

## Cold Path

Write transactions to Delta Lake for:

- Historical analytics
- Reconciliation workflows
- Business reporting

---

# Phase 7: Batch Analytics

Future Implementation.

File:

```
src/batch/reconciliation_job.py
```

Objective:

Create Spark batch jobs on top of Delta Lake data.

Example analytics:

- Total transaction volume by merchant
- Revenue by currency
- Successful vs failed transactions
- Daily settlement reports

Purpose:

Demonstrate why analytical workloads are better suited for Delta Lake compared with operational databases.

---

# Phase 8: Testing and CI/CD

Future Implementation.

## Testing

Add automated tests using:

- Pytest
- Data validation checks
- Transformation testing

## CI/CD

Add GitHub Actions workflow:

```
Code Push
    |
    v
GitHub Actions
    |
    v
Docker Environment
    |
    v
Run Tests
    |
    v
Build Validation
```

Goals:

- Automated testing
- Code quality checks
- Reliable deployments

---

# Engineering Concepts Demonstrated

- Event-driven architecture
- Lambda Architecture
- Stream processing
- Batch processing
- Distributed data systems
- Kafka-based ingestion
- Cassandra data modeling
- Delta Lake architecture
- Containerized development
- Automated testing and CI/CD

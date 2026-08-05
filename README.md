# Fintech Real-Time Transaction Lakehouse

A distributed data platform simulating the backend of a high-volume payment processing system.

This project demonstrates modern data engineering patterns for handling real-time transaction processing and large-scale financial analytics. The platform ingests simulated transaction events, processes them through streaming pipelines, performs real-time fraud checks, and supports batch reconciliation workflows.

## Overview

The system uses a Lambda Architecture approach, separating workloads based on latency and processing requirements:

- Real-time processing for fraud detection and transaction validation
- Batch processing for historical analytics and financial reconciliation

## Architecture

```
Transaction Generator
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
(Hot Path) (Cold Path)
   |         |
   v         v
Fraud      Financial
Checks     Analytics
```

## Real-Time Processing (Hot Path)

Technology: Apache Cassandra

Cassandra is used for low-latency transaction access patterns such as:

- Retrieving recent customer transactions
- Supporting fraud detection rules
- Handling high-volume transaction writes

The data model is designed around query patterns requiring predictable read performance.

## Batch Analytics (Cold Path)

Technology: Delta Lake with Apache Spark

Delta Lake is used for analytical workloads including:

- End-of-day transaction reconciliation
- Merchant settlement analysis
- Historical transaction reporting

It provides:

- ACID transactions
- Schema enforcement
- Reliable batch processing
- Spark SQL optimization

## Data Flow

```
Python Transaction Generator
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
 Cassandra   Delta Lake
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ |
| Streaming | Apache Kafka |
| Processing | PySpark Structured Streaming |
| Real-time Storage | Apache Cassandra |
| Lakehouse Storage | Delta Lake |
| Containerization | Docker, Docker Compose |
| Testing | Pytest |

## Project Structure

```
fintech-realtime-lakehouse/

├── docker/
│   └── Docker configurations

├── src/
│   ├── generators/
│   │   └── Synthetic transaction generator
│   │
│   ├── streaming/
│   │   └── PySpark streaming jobs
│   │
│   ├── batch/
│   │   └── Delta Lake processing jobs
│   │
│   ├── schemas/
│   │   └── Data contracts
│   │
│   └── utils/
│       └── Common utilities and connectors

├── tests/
├── scripts/
├── docs/
└── README.md
```

## Future Improvements

- Add Kafka Schema Registry for schema evolution
- Containerize Spark applications
- Add Apache Airflow orchestration
- Implement data quality checks using Great Expectations
- Add monitoring and observability
- Add CI/CD automation

## Engineering Concepts Demonstrated

- Event-driven architecture
- Lambda Architecture
- Stream and batch processing patterns
- Distributed data modeling
- Data lakehouse architecture
- Scalable ETL pipelines

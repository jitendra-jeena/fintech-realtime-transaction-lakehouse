Fintech Real-Time Transaction Lakehouse
A production-grade, distributed data platform simulating the backend of a high-volume payment gateway (e.g., Stripe/Razorpay). This system ingests millions of simulated financial transactions, routes them through Apache Kafka, and processes them using PySpark to serve two distinct business needs: real-time fraud detection and batch financial reconciliation.

🏗 Architecture & Trade-offs
This project demonstrates the "Lambda Architecture" pattern using modern open-source tools, specifically solving the challenge of querying the same data for two drastically different use cases.

The Hot Path (Real-Time):
Tech: Apache Cassandra
Use Case: Real-time fraud detection. When a card is swiped, we need to check the user's last 5 transactions in < 50ms to approve or decline.
Why Cassandra: Relational databases bottleneck on high-concurrency writes and point lookups at this scale. Cassandra provides predictable, single-digit-millisecond latency for exactly this pattern.
The Cold Path (Batch Analytics):
Tech: Delta Lake (on local/file storage)
Use Case: End-of-day financial reconciliation and merchant payout analytics.
Why Delta Lake: Running complex GROUP BY aggregations across millions of historical transactions would crash a Cassandra cluster (or require massive, expensive compute). Delta Lake allows for highly optimized, ACID-compliant batch SQL queries via Spark.
Data Flow:Python Data Generator ➡️ Apache Kafka (Topic) ➡️ PySpark Structured Streaming ➡️ Splits to: Cassandra (Hot) & Delta Lake (Cold)

🛠 Tech Stack
Streaming Broker: Apache Kafka
Processing: PySpark (Structured Streaming & Batch SQL)
Hot Storage: Apache Cassandra
Cold Storage: Delta Lake
Containerization: Docker & Docker Compose
Testing: Pytest
Language: Python 3.10+
📁 Project Structure
├── docker/                 # Docker Compose & Dockerfiles for Kafka, Cassandra, etc.├── src/                    # Application source code│   ├── generators/         # Scripts to simulate realistic payment transaction data│   ├── streaming/          # PySpark streaming jobs (Kafka to Cassandra/Delta)│   ├── batch/              # PySpark batch jobs (Delta Lake aggregations)│   ├── schemas/            # Explicit PySpark schemas to enforce data contracts│   └── utils/              # Shared configs, logging, and database connectors├── tests/                  # Unit and integration tests├── scripts/                # Bash scripts to spin up the environment easily├── docs/                   # Architecture diagrams├── .gitignore              # Ignores heavy data, delta logs, and Python cache└── README.md               # This file
🚀 Quick Start (Coming Soon)
Instructions to spin up the Docker containers and run the pipeline will be added here once the infrastructure is built.

📈 Future Improvements
 Add schema registry (Confluent/Apicurio) for strict Kafka schema evolution
 Containerize the PySpark jobs
 Add Airflow orchestration for the batch reconciliation jobs
 Implement data quality checks (Great Expectations) before writing to Delta Lake

# Docker Compose Deep Dive

## Overview

This document explains the Docker Compose infrastructure used in the FinTech Real-Time Transaction Lakehouse project.

The goal is to understand:

- How Docker containers are created
- Difference between images and containers
- Container networking
- Port mapping
- Runtime configuration using environment variables
- Kafka, ZooKeeper, and Cassandra setup decisions

---

# 1. Docker Fundamentals

## What is a Container?

A container is an isolated runtime environment that packages an application along with all its dependencies.

Containers solve common problems:

- Dependency conflicts
- Different development environments
- Application isolation
- Deployment consistency

Example:

Without containers:

```
Application
 |
 ├── Python version conflict
 ├── Library version conflict
 └── System dependency issues
```

With containers:

```
Container A
------------
Python Application
Python 3.10
Required libraries


Container B
------------
Kafka
ZooKeeper
```

Each container runs independently.

---

# 2. Docker Image vs Container

## Image

An image is a blueprint/template used to create containers.

Example:

```yaml
image: cassandra:latest
```

When running:

```bash
docker compose up
```

Docker follows this process:

```
docker compose up

        |
        v

Check local Docker image cache

        |
        +-------------+
        |             |
      Found        Not Found
        |             |
        v             v

 Use existing     Pull image from Docker Hub

                       |
                       v

                Create container
```

---

## Container

A container is a running instance created from an image.

Example:

```
Image:

cassandra:latest


creates:


Container:

cassandra
```

---

# 3. Docker Compose Services

Example:

```yaml
services:
  kafka:
```

The service name defines:

- Docker Compose service
- Internal DNS hostname
- Network identity

Example:

```
Kafka Container

       |
       |
       v

zookeeper:2181

       |
       |
       v

ZooKeeper Container
```

Docker automatically resolves service names.

---

# 4. Container Name

Example:

```yaml
container_name: kafka
```

Defines the actual running container name.

Used with:

```bash
docker ps

docker logs kafka

docker exec -it kafka bash
```

Difference:

```
Service name:
kafka


Container name:
kafka
```

They can be different.

---

# 5. Environment Variables

Example:

```yaml
environment:
  KAFKA_BROKER_ID: 1
```

Environment variables provide runtime configuration to applications inside containers.

The Docker image stays the same.

Example:

Same image:

```
confluentinc/cp-kafka:7.5.0
```

Different environments:

Development:

```
KAFKA_BROKER_ID=1
```

Production:

```
KAFKA_BROKER_ID=5
```

The application reads these values during startup.

---

# 6. Docker Networking

Containers communicate using service names.

Example:

```yaml
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

Means:

```
Kafka Container

      |
      |
      v

ZooKeeper Container

Hostname:
zookeeper

Port:
2181
```

Docker provides internal DNS resolution.

---

## Important localhost Concept

Inside a container:

```
localhost
```

means:

```
The same container
```

It does NOT mean another container.

Example:

Wrong:

```
Kafka Container

localhost:2181
```

Kafka will search inside itself.

Correct:

```
zookeeper:2181
```

Kafka connects to the ZooKeeper container.

---

# 7. Port Mapping

Docker port format:

```yaml
ports:
  - "HOST_PORT:CONTAINER_PORT"
```

Example:

```yaml
ports:
  - "9092:9092"
```

Means:

```
Local Machine

localhost:9092

        |
        |
        v

Kafka Container

9092
```

---

Port mapping is required when:

- Application runs outside Docker
- Laptop/VM needs access
- External clients need connection

---

If applications are inside the same Docker network:

Example:

```
Python Container

       |
       |
       v

Kafka Container
```

Connection:

```
kafka:9092
```

No port exposure is required.

---

# 8. ZooKeeper Configuration

## Client Port

```yaml
ZOOKEEPER_CLIENT_PORT: 2181
```

ZooKeeper listens for client connections on port 2181.

Kafka connects using:

```
zookeeper:2181
```

---

## Tick Time

```yaml
ZOOKEEPER_TICK_TIME: 2000
```

Value:

```
2000 milliseconds = 2 seconds
```

Used as ZooKeeper's internal timing unit for:

- Heartbeats
- Session management
- Distributed coordination

---

# 9. Kafka Configuration

## Broker ID

```yaml
KAFKA_BROKER_ID: 1
```

A Kafka broker is a Kafka server.

In production:

```
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Each broker requires a unique ID.

Example:

```yaml
Broker 1:
KAFKA_BROKER_ID=1

Broker 2:
KAFKA_BROKER_ID=2

Broker 3:
KAFKA_BROKER_ID=3
```

---

## ZooKeeper Connection

```yaml
KAFKA_ZOOKEEPER_CONNECT:
zookeeper:2181
```

Kafka connects to ZooKeeper using:

```
hostname:port
```

Example:

```
zookeeper:2181
```

---

## Advertised Listener

```yaml
KAFKA_ADVERTISED_LISTENERS:
PLAINTEXT://localhost:9092
```

Kafka tells clients:

"Use this address to connect to me."

Example:

External application:

```
localhost:9092
```

Docker application:

```
kafka:9092
```

---

## PLAINTEXT

```
PLAINTEXT
```

means:

- No encryption
- No SSL/TLS

Used commonly in development environments.

Production usually uses:

- SSL
- SASL authentication

---

## Offset Replication Factor

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Kafka stores consumer progress in:

```
__consumer_offsets
```

Example:

```
Consumer processed:

Transaction 1
Transaction 2
Transaction 3
```

Kafka stores:

```
Last processed offset = 3
```

Replication factor controls how many copies exist.

Development:

```
One broker
One copy
```

Production:

```
Broker 1
Broker 2
Broker 3
```

Multiple copies provide fault tolerance.

---

# 10. Cassandra Configuration

## Image

```yaml
image: cassandra:latest
```

Uses the official Cassandra Docker image.

Production recommendation:

Avoid:

```
cassandra:latest
```

Prefer:

```
cassandra:5.0
```

or a fixed version.

Reason:

Future deployments should use the same version.

---

## Cassandra Port

```yaml
ports:
  - "9042:9042"
```

9042 is Cassandra's CQL client port.

Applications connect using:

```python
Cluster(["localhost"], port=9042)
```

---

## Cassandra Cluster Name

```yaml
CASSANDRA_CLUSTER_NAME=FinTechCluster
```

Defines the Cassandra cluster identity.

Production example:

```
Cassandra Cluster

Node 1
 \
  \
   FinTechCluster
  /
 /
Node 2
```

All nodes must share the same cluster name.

---

# 11. Cassandra Health Check

Example:

```yaml
healthcheck:
  test:
    ["CMD", "cqlsh", "-e", "describe cluster"]
```

Docker executes:

```
Connect to Cassandra

Run:

describe cluster
```

If Cassandra responds:

```
Healthy
```

---

## Health Check Timing

### Interval

```yaml
interval: 30s
```

Run health check every 30 seconds.

---

### Timeout

```yaml
timeout: 10s
```

If Cassandra does not respond within 10 seconds:

```
Health check failed
```

---

### Retries

```yaml
retries: 5
```

Docker allows five failures before marking the container unhealthy.

---

# Final Architecture

```
                 Docker Network


              ZooKeeper
                  |
                  |
                  v

                Kafka
                  |
                  |
                  v

          PySpark Streaming
                  |
          +-------+-------+
          |               |
          v               v

     Cassandra        Delta Lake

      Hot Path        Analytics Path
```

This infrastructure supports:

- Real-time transaction ingestion
- Event-driven processing
- Low latency lookups
- Batch analytics
- Distributed data processing


import json
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from random import randint, choice, uniform
from confluent_kafka import Producer

# --- DATA MODEL ---
@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    merchant_id: str
    merchant_category: str
    payment_method: str
    timestamp: str
    is_fraud: bool

# --- KAFKA CONFIGURATION ---
# Why these settings? 
# - linger.ms: Waits 20ms to batch messages before sending. High throughput.
# - batch.size: 32KB. Works with linger.ms to optimize network requests.
# - compression.type: Snappy balances CPU usage and network bandwidth (critical for CDRs/Transactions).
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'fintech-producer-1',
    'linger.ms': 20,
    'batch.size': 32768,
    'compression.type': 'snappy'
}

producer = Producer(conf)

# --- DELIVERY CALLBACK ---
# In production, you MUST handle delivery reports to know if data actually reached Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    # else: # Uncomment only for local debugging, it will spam your console in production
    #     print(f"Delivered to topic {msg.topic()} partition [{msg.partition()}] at offset {msg.offset()}")

# --- DATA GENERATOR LOGIC ---
# Tier 1 companies want to see edge cases handled. A real-time fraud pipeline 
# is useless if you don't generate synthetic fraud to test it.
MERCHANT_CATEGORIES = ['grocery', 'electronics', 'travel', 'pharmacy', 'gas_station', 'online_gaming', 'jewelry']
HIGH_RISK_CATEGORIES = ['online_gaming', 'jewelry']

def generate_transaction() -> Transaction:
    is_fraud = randint(1, 100) <= 5 # 5% fraud rate
    category = choice(HIGH_RISK_CATEGORIES) if is_fraud else choice(MERCHANT_CATEGORIES)
    
    # Fraudulent transactions usually have distinct behavioral fingerprints (high amount, specific categories)
    amount = round(uniform(500.0, 5000.0), 2) if is_fraud else round(uniform(5.0, 200.0), 2)

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        user_id=f"user_{randint(1000, 9999)}",
        amount=amount,
        currency="USD",
        merchant_id=f"merch_{randint(100, 999)}",
        merchant_category=category,
        payment_method=choice(['credit', 'debit', 'upi']),
        timestamp=datetime.now(timezone.utc).isoformat(),
        is_fraud=is_fraud
    )

# --- EXECUTION ---
if __name__ == "__main__":
    topic_name = 'transactions'
    print(f"Starting producer. Sending data to '{topic_name}'...")
    
    try:
        # Simulating a continuous stream of events
        for i in range(1000):
            txn = generate_transaction()
            # asdict converts dataclass to dict, json.dumps makes it JSON (standard for Kafka payloads)
            producer.produce(
                topic=topic_name, 
                key=txn.user_id.encode('utf-8'), # Key ensures all transactions for a user go to the same partition (ordering guarantee)
                value=json.dumps(asdict(txn)).encode('utf-8'),
                callback=delivery_report
            )
            
            # Trigger delivery reports for queued messages
            producer.poll(0)
            
            # Control the rate of ingestion (e.g., ~100 msgs/sec)
            time.sleep(0.01)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Flush ensures all buffered messages are sent before exiting
        producer.flush()
        print("Producer flushed and terminated.")

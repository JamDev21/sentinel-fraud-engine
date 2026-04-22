import pandas as pd
import json
import os
import asyncio
from typing import Dict, Any
from aiokafka import AIOKafkaProducer

# --- 1. CONFIGURATION ---
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
TOPIC = "financial-transactions"
DATA_PATH = "data/shadow_test/shadow_test_data.csv"

async def run_shadow_stream(requests_per_second: int = 3) -> None:
    # Load the holdout dataset (Real distributions, not Faker)
    try:
        df = pd.read_csv(DATA_PATH)
    except FileNotFoundError:
        print(f"Error: Could not locate dataset at {DATA_PATH}.")
        return

    # Initialize the asynchronous Kafka Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    await producer.start()
    print(f"Data Replay Producer connected to Redpanda at {REDPANDA_BROKER}")
    print(f"Initiating stream with {len(df)} real patterns at {requests_per_second} req/sec...")
    print("-" * 75)

    try:
        while True:
            # Randomly sample one transaction from the CSV
            row = df.sample(1).iloc[0]
            
            # Construct the payload.
            # CRITICAL: We include the 'real_label_Fraud' as a hidden tracker.
            # The API will ignore it for prediction, but pass it downstream 
            # to PostgreSQL and the Results Topic for final evaluation.
            payload: Dict[str, Any] = {
                "type": int(row['type']),
                "amount": float(row['amount']),
                "oldbalanceOrg": float(row['oldbalanceOrg']),
                "newbalanceOrig": float(row['newbalanceOrig']),
                "oldbalanceDest": float(row['oldbalanceDest']),
                "newbalanceDest": float(row['newbalanceDest']),
                "hourOfDay": int(row['hourOfDay']),
                "errorBalanceOrig": float(row['errorBalanceOrig']),
                "errorBalanceDest": float(row['errorBalanceDest']),
                "fractionAmount": float(row['fractionAmount']),
                "real_label_Fraud": bool(row['real_label_Fraud']) 
            }

            # Fire and Forget into Redpanda
            await producer.send_and_wait(TOPIC, value=payload)
            
            # Simple console output (Since we can't calculate True Positives here anymore)
            status = "REAL FRAUD" if payload['real_label_Fraud'] else "🟢 LEGAL"
            print(f"[SENT] Amount: ${payload['amount']:>8.2f} | Ground Truth: {status}")
            
            # Control throughput
            await asyncio.sleep(1 / requests_per_second)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("\nStreaming halted by user.")
    finally:
        await producer.stop()
        print("Producer disconnected.")

if __name__ == "__main__":
    asyncio.run(run_shadow_stream(requests_per_second=3))
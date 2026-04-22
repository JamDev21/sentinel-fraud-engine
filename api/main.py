from contextlib import asynccontextmanager
from fastapi import FastAPI
import xgboost as xgb
import pandas as pd
import json
import os
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncpg
from datetime import datetime

# CONFIGURATION & GLOBALS
# Connection strings (defaults set for local development fallback)
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sentinel_admin:super_secret_password@localhost:5432/sentinel_db")

# Architecture topic definitions
TOPIC_RAW = "financial-transactions"
TOPIC_RESULTS = "fraud-results"

# Global clients mapped during the application lifespan
producer = None
db_pool = None

# Pre-load the ML model into memory on startup to avoid latency during inference
model = xgb.XGBClassifier()
model.load_model("models/sentinel_xgboost.json")

# STREAM PROCESSING ENGINE 
async def consume_and_predict():
    """
    Continuous background loop that listens to Redpanda, runs XGBoost inference, 
    and broadcasts the results to downstream components.
    """
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=REDPANDA_BROKER,
        # group_id is critical for horizontal auto-scaling 
        group_id="sentinel-inference-group", 
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    await consumer.start()
    try:
        print(f"Sentinel actively listening to topic: {TOPIC_RAW}")
        
        # Asynchronous event loop: processes messages as they arrive without blocking
        async for msg in consumer:
            transaction_dict = msg.value
            
            # A. ML Inference
            # Extract features expected by the model to prevent dimension errors
            df_input = pd.DataFrame([transaction_dict])[model.feature_names_in_]
            prediction = model.predict(df_input)
            probability = model.predict_proba(df_input)[0][1]
            
            is_fraud = bool(prediction[0])
            prob = round(float(probability) * 100, 2)

            result_payload = {
                **transaction_dict,
                "is_fraud": is_fraud,
                "fraud_probability": prob,
                "action_recommended": "BLOCK" if is_fraud else "ALLOW",
                "processed_at": str(datetime.now())
            }

            # B. Flow 3a: Publish enriched payload to Redpanda (consumed by Dashboard)
            if producer:
                await producer.send_and_wait(TOPIC_RESULTS, value=result_payload)
            
            # C. Flow 3b: Persist transaction audit log in PostgreSQL
            if db_pool:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO fraud_logs (transaction_data, is_fraud, fraud_probability) VALUES ($1, $2, $3)",
                        json.dumps(transaction_dict), is_fraud, prob
                    )
            
            # Console monitor
            print(f"Processed: {'FRAUD' if is_fraud else 'CLEAN'} ({prob}%)")

    finally:
        # Ensure consumer disconnects gracefully on fatal errors
        await consumer.stop()

# APPLICATION LIFECYCLE 
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown events for the FastAPI application, 
    ensuring infrastructure connections are safely handled.
    """
    global producer, db_pool
    
    # Initialize outbound infrastructure connections
    producer = AIOKafkaProducer(
        bootstrap_servers=REDPANDA_BROKER, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    print(f"Connected to Redpanda at {REDPANDA_BROKER}")
    
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    print("Connected to PostgreSQL successfully")

    # --- THE MISSING PIECE: SELF-HEALING DATABASE MIGRATION ---
    # Create table if it doesn't exist to prevent crash on first run
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS fraud_logs (
                id SERIAL PRIMARY KEY,
                transaction_data JSONB NOT NULL,
                is_fraud BOOLEAN NOT NULL,
                fraud_probability FLOAT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("Table 'fraud_logs' verified/created.")
    
    # Launch the consumer loop as an independent, non-blocking background task
    asyncio.create_task(consume_and_predict())
    
    yield # API is now fully online and serving requests
    
    # Teardown: close connections cleanly to prevent memory leaks or zombie processes
    if producer: 
        await producer.stop()
    if db_pool: 
        await db_pool.close()

# FASTAPI INITIALIZATION 
app = FastAPI(
    title="Sentinel Engine", 
    description="Event-driven fraud detection inference service.",
    lifespan=lifespan
)

@app.get("/health")
async def health():
    """Kubernetes liveness/readiness probe endpoint."""
    return {
        "status": "active", 
        "mode": "stream-consumer",
        "timestamp": str(datetime.now())
    }
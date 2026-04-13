from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import xgboost as xgb
import pandas as pd

app = FastAPI(
    title="Sentinel API",
    description="Real-time financial fraud detection engine using XGBoost.",
    version="1.0.0"
)

# Load the model globally at startup to ensure it resides in RAM,
# preventing loading delays during individual API requests.
model = xgb.XGBClassifier()
try:
    model.load_model("models/sentinel_xgboost.json")
    print("Sentinel model successfully loaded.")
except Exception as e:
    print(f"Error loading the model. Please check the path: {e}")

class Transaction(BaseModel):
    """
    Data schema for incoming financial transactions.
    Validates that requests contain the exact features engineered during training.
    """
    type: int
    amount: float
    errorBalanceOrig: float
    oldbalanceDest: float
    hourOfDay: int
    errorBalanceDest: float
    newbalanceDest: float
    fractionAmount: float
    oldbalanceOrg: float
    newbalanceOrig: float

@app.get("/")
def health_check():
    """Health check endpoint to monitor system status."""
    return {
        "status": "online",
        "message": "Sentinel is watching"
    }

@app.post("/predict")
def predict_fraud(transaction: Transaction):
    """
    Receives a single transaction, dynamically aligns features, 
    and returns a fraud probability alongside a business action recommendation.
    """
    try:
        df_input = pd.DataFrame([transaction.dict()])
        
        # Enforce exact column ordering expected by the XGBoost model 
        # to prevent 'feature_names mismatch' errors.
        df_input = df_input[model.feature_names_in_]
        
        # Calculate prediction (0: Legitimate, 1: Fraud) and its probability
        prediction = model.predict(df_input)
        probability = model.predict_proba(df_input)[0][1] 
        
        return {
            "is_fraud": bool(prediction[0]),
            "fraud_probability": round(float(probability) * 100, 2),
            "action_recommended": "BLOCK_TRANSACTION" if prediction[0] == 1 else "ALLOW"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
import pandas as pd
import requests
import time
from typing import Dict, Any

API_URL = "http://127.0.0.1:8000/predict"
DATA_PATH = "data/shadow_test/shadow_test_data.csv"

def run_infinite_shadow_stream(requests_per_second: int = 3) -> None:
    """
    Executes a shadow testing simulation by replaying real historical data 
    against the live Sentinel API. Tracks business-critical metrics in real-time.
    """
    # Load the holdout dataset. Using real data ensures we evaluate the model 
    # against actual fraud patterns rather than synthetic (Faker) anomalies.
    try:
        df = pd.read_csv(DATA_PATH)
    except FileNotFoundError:
        print(f"Error: Could not locate the dataset at {DATA_PATH}.")
        return

    print(f"Initiating Infinite Stream with {len(df)} real patterns...")
    print(f"Firing at {requests_per_second} requests/second...")
    print("-" * 75)
    
    # Advanced tracking metrics for the real-time confusion matrix
    total = 0
    true_positives = 0  # Correctly blocked fraud
    false_positives = 0 # Honest clients blocked by mistake
    false_negatives = 0 # Fraud that slipped through

    try:
        while True:
            # Simulate random arrival of transactions by sampling uniformly
            row = df.sample(1).iloc[0]
            
            # Explicitly cast pandas types to native Python types (int, float) 
            # to prevent JSON serialization errors in the FastAPI backend.
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
                "fractionAmount": float(row['fractionAmount'])
            }

            real_label = bool(row['real_label_Fraud'])
            start_time = time.time()
            
            try:
                # Network failsafe: 2-second timeout prevents the simulation 
                # from hanging indefinitely if the Docker container crashes.
                response = requests.post(API_URL, json=payload, timeout=2.0)
                latency = round((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    result = response.json()
                    prediction = result['is_fraud']
                    prob = result['fraud_probability']
                    
                    total += 1
                    
                    # Real-time Confusion Matrix logic and console formatting
                    if prediction and real_label:
                        true_positives += 1
                        print(f"[FRAUD STOPPED]  | Prob: {prob:>5.1f}% | Lat: {latency:>3}ms")
                    elif prediction and not real_label:
                        false_positives += 1
                        print(f"[FALSE ALARM]    | Prob: {prob:>5.1f}% | Lat: {latency:>3}ms")
                    elif not prediction and real_label:
                        false_negatives += 1
                        print(f"[FRAUD ESCAPED]  | Prob: {prob:>5.1f}% | Lat: {latency:>3}ms")
                    else:
                        print(f"[LEGAL APPROVED] | Prob: {prob:>5.1f}% | Lat: {latency:>3}ms")

                else:
                    print(f"API Error: HTTP {response.status_code}")

            except requests.exceptions.RequestException as e:
                # Catch connection drops without killing the main loop
                print(f"[CONNECTION ERROR] Waiting for container response... Details: {e}")

            # Control the throughput of the simulation
            time.sleep(1 / requests_per_second)

    except KeyboardInterrupt:
        # Graceful shutdown and final executive summary
        print("\nStreaming halted by user.")
        print("=" * 50)
        print("EXECUTIVE BUSINESS REPORT (SHADOW TEST)")
        print("=" * 50)
        print(f"Total transactions evaluated: {total}")
        
        if total > 0:
            print(f"Fraud Stopped (True Positives):    {true_positives}")
            print(f"Fraud Escaped (False Negatives):   {false_negatives}")
            print(f"False Alarms (False Positives):    {false_positives}")
            
            # Safe recall calculation preventing division by zero
            recall = (true_positives / (true_positives + false_negatives) * 100) if (true_positives + false_negatives) > 0 else 0
            print(f"\nModel Effectiveness (Recall): {recall:.1f}% of attacks blocked.")
        print("=" * 50)

if __name__ == "__main__":
    run_infinite_shadow_stream(requests_per_second = 3)
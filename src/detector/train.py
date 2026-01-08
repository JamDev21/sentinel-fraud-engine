# importing libraries 
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from imblearn.over_sampling import SMOTE
import joblib
import os
from pathlib import Path


# Configuration of relative paths
BASE_DATA_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DATA_DIR.parent.parent / "data" / "raw" / "creditcard.csv"
MODEL_DIR = BASE_DATA_DIR.parent.parent / "models"
MODEL_PATH = MODEL_DIR / "fraud_xgboost.joblib"
RANDOM_STATE = 42

def load_data(path):
    """Load dataset CSV"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"ERROR: The file was not found in the path {path}")
    
    print("Loading data...")
    df = pd.read_csv(path)
    print(f"Data loaded : {df.shape[0]} rows")
    return df
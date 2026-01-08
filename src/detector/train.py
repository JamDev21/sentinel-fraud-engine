# importing libraries 
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from imblearn.over_sampling import SMOTE
import joblib
import os

# Configuration of relative paths
BASE_DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(
    BASE_DATA_DIR,
    "..",
    "..",
    "data",
    "raw",
    "creditcard.csv"
    )
MODEL_DIR = "../../models"
MODEL_PATH = os.path.join(MODEL_DIR, "fraud_xgboost.joblib")
RANDOM_STATE = 42

def load_data(path):
    """Load dataset CSV"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"ERROR: The file was not found in the path {path}")
    
    print("Loading data...")
    df = pd.read_csv(path)
    print(f"Data loaded : {df.shape[0]} rows")
    return df

df = load_data(DATA_PATH)
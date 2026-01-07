# importing libraries 
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from imblearn.over_sampling import SMOTE
import joblib
import os

# Configuration of relative paths
DATA_PATH = "../../data/raw/creditcard.csv"
MODEL_DIR = "../../models"
MODEL_PATH = os.path.join(MODEL_DIR, "fraud_xgboost.joblib")
RANDOM_STATE = 42

def load_data(path):
    """Load dataset CSV"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"ERROR: The file was not found in the path {path}")
    
    print("Loading data...")
    df = pd.read.csv(path)
    print(f"Data loaded : [df.shape[0]] rows")
    return df

import pandas as pd
from fyers_apiv3 import fyersModel
import json
import os
import time
import pytz
import pandas_ta as ta  # Import pandas_ta for technical indicators
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import re

# Fyers API credentials
CLIENT_ID = "EZVL3AY2L8-100"
SECRET_KEY = "X0L3VGF0BU"
REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"
TOKEN_PATH = "C:/Users/user/Downloads/Compressed/store_token.json"

# Function to load token from the file
def load_token():
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'r') as f:
            return json.load(f)
    return None

# Function to save token to the file
def save_token(token_data):
    with open(TOKEN_PATH, 'w') as f:
        json.dump(token_data, f)

# Function to connect to Fyers API using authorization code
def connect_fyers(auth_code=None):
    token_data = load_token()
    
    if token_data and token_data.get("access_token"):
        print("Using existing access token")
        return fyersModel.FyersModel(client_id=CLIENT_ID, token=token_data['access_token'], log_path="./logs/")
    
    if auth_code:
        session = fyersModel.SessionModel(
            client_id=CLIENT_ID,
            secret_key=SECRET_KEY,
            redirect_uri=REDIRECT_URI,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get("s") == "error":
            print("Error:", response)
            return None
        
        access_token = response.get("access_token")
        save_token(response)
        print("Access Token Generated:", access_token)
        return fyersModel.FyersModel(client_id=CLIENT_ID, token=access_token, log_path="./logs/")
    else:
        print("Authorization code required for first-time login.")
        return None

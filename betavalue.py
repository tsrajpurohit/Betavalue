import yfinance as yf
import numpy as np
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import time

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets():
    try:
        credentials_json = os.getenv("GOOGLE_SHEET_CREDENTIALS")  # Get credentials from GitHub Secrets
        credentials_info = json.loads(credentials_json)
        credentials = Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        print(f"Error: {e}")
        return None

# Function to create a new worksheet if it doesn't exist
def create_or_get_worksheet(sheet, worksheet_name):
    try:
        try:
            worksheet = sheet.worksheet(worksheet_name)
            print(f"Worksheet '{worksheet_name}' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="2")
            print(f"Worksheet '{worksheet_name}' created.")
        return worksheet
    except Exception as e:
        print(f"Error creating or accessing worksheet: {e}")
        return None

# Function to update the Google Sheet with beta values
def update_google_sheet(worksheet, data):
    try:
        values = [["Stock", "Beta"]] + data
        worksheet.clear()
        worksheet.update("A1", values)
        print("Beta values uploaded to Google Sheets successfully.")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")

# Function to calculate beta
def calculate_beta(stock, index, period="1y"):
    try:
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']
        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        # Align data lengths
        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        covariance = np.cov(returns_stock, returns_index)[0][1]
        variance = np.var(returns_index)
        beta = covariance / variance
        return beta
    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

if __name__ == "__main__":
    sheet_id = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Google Sheet ID
    client = authenticate_google_sheets()
    if not client:
        raise ValueError("Google Sheets authentication failed.")
    
    sheet = client.open_by_key(sheet_id)
    worksheet_name = "Beta Values"
    worksheet = create_or_get_worksheet(sheet, worksheet_name)
    if not worksheet:
        raise ValueError("Failed to get or create the worksheet.")

    stocks = fnolist()  # Fetch F&O stock symbols
    index = "^NSEI"  # Nifty 50 Index

    beta_data = []
    for stock in stocks:
        print(f"Processing stock: {stock}")
        beta = calculate_beta(stock, index, period="1y")
        if beta is not None:
            print(f"{stock}: {beta}")
            beta_data.append([stock, beta])
        else:
            print(f"Skipping {stock} due to calculation error.")
        
        time.sleep(1)

    if beta_data:
        update_google_sheet(worksheet, beta_data)
    else:
        print("No beta data to upload.")

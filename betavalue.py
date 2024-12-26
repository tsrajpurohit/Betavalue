import os
import yfinance as yf
import numpy as np
import talib
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import json
import time

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

# Function to calculate beta using TA-Lib
def calculate_beta_with_talib(stock, index, period="1y"):
    try:
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']

        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        beta = talib.LINEARREG_SLOPE(returns_stock.values, timeperiod=min_len)
        return beta[-1]  # Return the last calculated beta value
    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

if __name__ == "__main__":
    # Fetch credentials and Sheet ID from environment variables
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
    SHEET_ID = os.getenv('GOOGLE_SHEET_ID')  # Sheet ID

    if not credentials_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable is not set.")

    # Authenticate using the JSON string from environment
    credentials_info = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(credentials)

    # Open the sheet using the Sheet ID
    sheet = client.open_by_key(SHEET_ID)

    # Name of the worksheet
    worksheet_name = "Beta Values"
    worksheet = create_or_get_worksheet(sheet, worksheet_name)
    if not worksheet:
        raise ValueError("Failed to get or create the worksheet.")

    # Fetch all F&O stock symbols
    stocks = fnolist()
    index = "^NSEI"  # Nifty 50 Index

    # Calculate beta for each stock
    beta_data = []
    for stock in stocks:
        print(f"Processing stock: {stock}")
        beta = calculate_beta_with_talib(stock, index, period="1y")
        if beta is not None:
            print(f"{stock}: {beta}")
            beta_data.append([stock, beta])
        else:
            print(f"Skipping {stock} due to calculation error.")
        time.sleep(1)

    # Update Google Sheet
    if beta_data:
        update_google_sheet(worksheet, beta_data)
    else:
        print("No beta data to upload.")

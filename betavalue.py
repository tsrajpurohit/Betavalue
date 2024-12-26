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
        print(f"Checking if worksheet '{worksheet_name}' exists...")
        try:
            worksheet = sheet.worksheet(worksheet_name)
            print(f"Worksheet '{worksheet_name}' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{worksheet_name}' not found. Creating a new worksheet...")
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="2")
            print(f"Worksheet '{worksheet_name}' created successfully.")
        return worksheet
    except Exception as e:
        print(f"Error creating or accessing worksheet: {e}")
        return None

# Function to update the Google Sheet with beta values
def update_google_sheet(worksheet, data):
    try:
        print("Preparing data for upload to Google Sheets...")
        values = [["Stock", "Beta"]] + data
        print("Clearing existing data in the worksheet...")
        worksheet.clear()
        print("Uploading beta values to Google Sheets...")
        worksheet.update("A1", values)
        print("Beta values uploaded to Google Sheets successfully.")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")

# Function to calculate beta using TA-Lib
def calculate_beta_with_talib(stock, index, period="1y"):
    try:
        print(f"Fetching data for stock: {stock} and index: {index} (period: {period})...")
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']

        print(f"Calculating daily returns for stock: {stock}...")
        returns_stock = stock_data.pct_change().dropna()
        print(f"Calculating daily returns for index: {index}...")
        returns_index = index_data.pct_change().dropna()

        print("Aligning data lengths for beta calculation...")
        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        print("Using TA-Lib to calculate beta...")
        beta = talib.LINEARREG_SLOPE(returns_stock.values, timeperiod=min_len)
        print(f"Beta for {stock} calculated successfully: {beta[-1]}")
        return beta[-1]  # Return the last calculated beta value
    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

if __name__ == "__main__":
    # Fetch credentials and Sheet ID from environment variables
    print("Fetching Google Sheets credentials from environment variables...")
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
    SHEET_ID = os.getenv('GOOGLE_SHEET_ID')  # Sheet ID

    if not credentials_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable is not set.")

    print("Authenticating with Google Sheets...")
    credentials_info = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(credentials)
    print("Authentication successful.")

    print(f"Opening Google Sheet with ID: {SHEET_ID}...")
    sheet = client.open_by_key(SHEET_ID)

    # Name of the worksheet
    worksheet_name = "Beta Values"
    print(f"Checking worksheet: {worksheet_name}...")
    worksheet = create_or_get_worksheet(sheet, worksheet_name)
    if not worksheet:
        raise ValueError("Failed to get or create the worksheet.")

    print("Fetching list of F&O stock symbols...")
    stocks = fnolist()
    print(f"Total stocks fetched: {len(stocks)}")
    index = "^NSEI"  # Nifty 50 Index

    print("Starting beta calculation for each stock...")
    beta_data = []
    for i, stock in enumerate(stocks, start=1):
        print(f"[{i}/{len(stocks)}] Processing stock: {stock}")
        beta = calculate_beta_with_talib(stock, index, period="1y")
        if beta is not None:
            beta_data.append([stock, beta])
        else:
            print(f"Skipping stock: {stock} due to calculation error.")
        print(f"Waiting for 1 second to avoid hitting API rate limits...")
        time.sleep(1)

    if beta_data:
        print("Updating Google Sheets with beta data...")
        update_google_sheet(worksheet, beta_data)
    else:
        print("No beta data to upload.")
    print("Process completed.")

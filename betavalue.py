import os
import json
import yfinance as yf
import numpy as np
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import time
import random

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(credentials_json=None):
    try:
        if credentials_json is None:
            credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")  # Environment variable holding the JSON string
            if not credentials_json:
                raise ValueError("Credentials not found in environment variables.")
        
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
        # Try to get the worksheet by name
        try:
            worksheet = sheet.worksheet(worksheet_name)
            print(f"Worksheet '{worksheet_name}' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            # If the worksheet doesn't exist, create it
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="100", cols="2")
            print(f"Worksheet '{worksheet_name}' created.")
        return worksheet
    except Exception as e:
        print(f"Error creating or accessing worksheet: {e}")
        return None

# Function to update the Google Sheet with beta values
def update_google_sheet(worksheet, data):
    try:
        # Prepare the data in the format [["Stock", "Beta"], ...]
        values = [["Stock", "Beta"]] + data
        worksheet.clear()  # Clear the existing data
        worksheet.update("A1", values)  # Update the sheet starting at cell A1
        print("Beta values uploaded to Google Sheets successfully.")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")

# Function to calculate beta
def calculate_beta(stock, index, period="1y"):
    try:
        # Download stock and index data
        stock_data = safe_yf_download(f"{stock}.NS", period)
        index_data = safe_yf_download(index, period)

        if stock_data is None or index_data is None:
            return None

        # Calculate daily returns
        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        # Align data lengths
        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        # Calculate beta
        covariance = np.cov(returns_stock, returns_index)[0][1]
        variance = np.var(returns_index)
        beta = covariance / variance
        return beta
    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

# Function to safely download data with retries
def safe_yf_download(ticker, period="1y"):
    tries = 3
    for i in range(tries):
        try:
            return yf.download(f"{ticker}", period=period)['Close']
        except Exception as e:
            print(f"Error downloading data for {ticker}, attempt {i + 1}/{tries}: {e}")
            if i < tries - 1:
                time.sleep(random.uniform(2, 5))  # Randomized sleep to avoid hitting rate limits
            else:
                return None  # Give up after retries

# Main script execution
if __name__ == "__main__":
    # Load credentials from environment variable
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    if not credentials_json:
        raise ValueError("Credentials not found in environment variables.")
    
    # Google Sheets ID (ensure it's correctly set in your environment)
    sheet_id = os.getenv("SHEET_ID", "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY")  # Default to a hardcoded ID if not set

    # Authenticate with Google Sheets
    client = authenticate_google_sheets(credentials_json)
    if not client:
        raise ValueError("Google Sheets authentication failed.")
    
    sheet = client.open_by_key(sheet_id)  # Open the sheet by ID

    # Name of the worksheet to be created or accessed
    worksheet_name = "Beta Values"  # Change this to whatever name you'd like

    # Create or get the worksheet
    worksheet = create_or_get_worksheet(sheet, worksheet_name)
    if not worksheet:
        raise ValueError("Failed to get or create the worksheet.")

    # Fetch all F&O stock symbols
    stocks = fnolist()
    index = "^NSEI"  # Nifty 50 Index

    # Calculate beta for each stock and store results in a list
    beta_data = []
    for stock in stocks:
        print(f"Processing stock: {stock}")
        beta = calculate_beta(stock, index, period="1y")
        if beta is not None:
            print(f"{stock}: {beta}")
            beta_data.append([stock, beta])  # Store the result in a list
        else:
            print(f"Skipping {stock} due to calculation error.")
        
        # Add delay to avoid hitting API rate limits
        time.sleep(1)  # Sleep for 1 second between requests

    # Update Google Sheet with the beta data
    if beta_data:
        update_google_sheet(worksheet, beta_data)
    else:
        print("No beta data to upload.")

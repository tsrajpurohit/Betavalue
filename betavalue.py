import os
import json
import yfinance as yf
import numpy as np
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import csv

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Assuming the Sheet ID is stored as an environment variable

if not credentials_json or not SHEET_ID:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS or SHEET_ID environment variables are not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

# Open the Google Sheet by ID
sheet = client.open_by_key(SHEET_ID)

def calculate_beta(stock, index, period="1y"):
    try:
        # Download stock and index data
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']

        # Check for NaN values in the stock and index data
        if stock_data.isna().any() or index_data.isna().any():
            print(f"Error: Data for {stock} or {index} contains NaN values.")
            return None

        # Calculate daily returns
        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        # Check if there is enough data for calculating beta
        if len(returns_stock) < 2 or len(returns_index) < 2:
            print(f"Error: Not enough data for {stock} or {index} to calculate beta.")
            return None

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
        return None  # Return None if there's an error

if __name__ == "__main__":
    # Fetch all F&O stock symbols
    stocks = fnolist()
    index = "^NSEI"  # Nifty 50 Index

    # Calculate beta for each stock
    beta_values = []
    failed_stocks = []  # List to keep track of stocks that failed
    for stock in stocks:
        beta = calculate_beta(stock, index, period="1y")
        if beta is not None:
            print(f"{stock}: {beta}")
            beta_values.append([stock, beta])
        else:
            failed_stocks.append(stock)  # Add stock to the failed list if it failed

    # Check if beta_values is not empty before attempting to write to Google Sheets
    if beta_values:
        # Create a new tab in Google Sheets for Beta values
        new_worksheet = sheet.add_worksheet(title="Beta Values", rows="1000", cols="2")
        
        # Add headers in the new tab
        new_worksheet.update('A1', 'Stock Symbol')
        new_worksheet.update('B1', 'Beta Value')

        # Write beta values to the new tab in Google Sheets
        new_worksheet.append_rows(beta_values, value_input_option="RAW")

        print("Beta values have been written to Google Sheets.")
    else:
        print("No valid beta values to write to Google Sheets.")

    # Save beta values to a CSV file if there are any
    if beta_values:
        with open("beta_values.csv", mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Stock Symbol", "Beta Value"])  # Write headers
            writer.writerows(beta_values)  # Write the data

        print("Beta values have been saved to 'beta_values.csv'.")

    # Report failed stocks (if any)
    if failed_stocks:
        print(f"The following stocks failed to calculate beta: {', '.join(failed_stocks)}")

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
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Assuming the Sheet ID is also stored as an environment variable

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
        return f"Error: {e}"

if __name__ == "__main__":
    # Fetch all F&O stock symbols
    stocks = fnolist()
    index = "^NSEI"  # Nifty 50 Index

    # Calculate beta for each stock
    beta_values = []
    for stock in stocks:
        beta = calculate_beta(stock, index, period="1y")  # Use a valid period
        print(f"{stock}: {beta}")
        
        # Store beta value in list for Google Sheets and CSV
        beta_values.append([stock, beta])

    # Create a new tab in Google Sheets for Beta values
    new_worksheet = sheet.add_worksheet(title="Beta Values", rows="1000", cols="2")
    
    # Add headers in the new tab
    new_worksheet.update('A1', 'Stock Symbol')
    new_worksheet.update('B1', 'Beta Value')

    # Write beta values to the new tab in Google Sheets
    new_worksheet.append_rows(beta_values, value_input_option="RAW")

    # Save beta values to a CSV file
    with open("beta_values.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Stock Symbol", "Beta Value"])  # Write headers
        writer.writerows(beta_values)  # Write the data

    print("Beta values have been written to Google Sheets and saved to 'beta_values.csv'.")

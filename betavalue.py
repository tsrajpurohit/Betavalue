import yfinance as yf
import numpy as np
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import time
import csv
import math
import pandas as pd
# Function to create or get the worksheet
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
        print("Beta values uploaded successfully.")
    except Exception as e:
        print(f"Error updating Google Sheets: {e}")

# Function to save beta values to a CSV file
def save_to_csv(file_name, data):
    try:
        with open(file_name, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Stock", "Beta"])  # Add header
            writer.writerows(data)
        print(f"Beta values saved to {file_name} successfully.")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

# Function to calculate beta
# Function to calculate beta
def calculate_beta(stock, index, period="1y"):
    try:
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']

        # Check if there is enough data for both stock and index
        if len(stock_data) < 2 or len(index_data) < 2:
            print(f"Not enough data for stock {stock} or index {index}.")
            return None

        # Calculate returns for stock and index
        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        # Ensure we have enough data after dropping NaNs
        if len(returns_stock) < 2 or len(returns_index) < 2:
            print(f"Not enough returns data for stock {stock} or index {index}.")
            return None

        # Use the minimum length to avoid mismatched data
        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        # Calculate covariance and variance
        covariance = np.cov(returns_stock, returns_index)[0][1]
        variance = np.var(returns_index)

        # Handle zero variance to avoid divide by zero error
        if variance == 0:
            print(f"Zero variance for index data: {index}")
            return None

        beta = covariance / variance

        # Ensure beta is a valid float
        if math.isinf(beta) or math.isnan(beta):
            print(f"Invalid beta value for {stock}: {beta}")
            return None

        return float(beta)  # Convert to a plain float

    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

if __name__ == "__main__":
    # Fetch credentials and Sheet ID from environment variables
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
    SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

    if not credentials_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

    # Authenticate using the JSON string from environment
    credentials_info = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(credentials)

    # Open the Google Sheet by ID
    sheet = client.open_by_key(SHEET_ID)

    # Worksheet name
    worksheet_name = "Beta Values"
    
    # Output CSV file name
    output_csv = "beta_values.csv"

    # Create or get the worksheet
    worksheet = create_or_get_worksheet(sheet, worksheet_name)
    if not worksheet:
        raise ValueError("Failed to get or create the worksheet.")

    # Fetch all F&O stock symbols
    stocks = fnolist()
    index = "^NSEI"

    # Calculate beta for each stock
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

    # Save beta data to CSV
    if beta_data:
        save_to_csv(output_csv, beta_data)

        # Update Google Sheet with the beta data
        update_google_sheet(worksheet, beta_data)
    else:
        print("No beta data to save or upload.")

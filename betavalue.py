import yfinance as yf
import numpy as np
from nsepython import fnolist
import gspread
from google.oauth2.service_account import Credentials
import json
import time

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(credentials_json):
    try:
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
        print(f"Error calculating beta for {stock}: {e}")
        return None

if __name__ == "__main__":
    # Absolute path for credentials JSON file
   # Fetch credentials from environment variables
   credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string from environment

   if not credentials_json:
      raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

    # Read the credentials JSON from the absolute path
    try:
        with open(credentials_path, "r") as file:
            credentials_json = file.read()
    except FileNotFoundError:
        raise ValueError(f"Credentials file not found at {credentials_path}")

    # Absolute Sheet ID (this should be the actual ID of your Google Sheet)
    sheet_id = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"  # Hardcoded Sheet ID

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

import time
import yfinance as yf
import numpy as np
import pandas as pd

def calculate_beta(stock, index, period="1y"):
    try:
        # Download stock and index data
        stock_data = yf.download(f"{stock}.NS", period=period)['Close']
        index_data = yf.download(index, period=period)['Close']

        # Log data to check what was fetched
        print(f"Fetched data for {stock}:")
        print(stock_data.head())
        print(f"Fetched data for {index}:")
        print(index_data.head())

        # Check if data is empty
        if stock_data.empty or index_data.empty:
            print(f"Data is missing for {stock} or {index}. Skipping.")
            return None

        # Calculate daily returns
        returns_stock = stock_data.pct_change().dropna()
        returns_index = index_data.pct_change().dropna()

        # Log return lengths to verify data
        print(f"Returns for {stock}: {returns_stock.head()}")
        print(f"Returns for {index}: {returns_index.head()}")

        # Ensure we have enough data (at least 2 data points)
        if len(returns_stock) < 2 or len(returns_index) < 2:
            print(f"Not enough data to calculate beta for {stock}.")
            return None

        # Align data lengths
        min_len = min(len(returns_stock), len(returns_index))
        returns_stock = returns_stock[-min_len:]
        returns_index = returns_index[-min_len:]

        # Calculate covariance and variance
        covariance = np.cov(returns_stock, returns_index)[0][1]
        variance = np.var(returns_index)

        # Check for division by zero in variance
        if variance == 0:
            print(f"Variance of {index} is zero for {stock}, skipping.")
            return None

        # Calculate beta
        beta = covariance / variance
        return beta
    except Exception as e:
        print(f"Error calculating beta for {stock}: {e}")
        return None

def process_stocks(stocks, index, output_file="beta_values.csv"):
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

    # Save results to CSV
    if beta_data:
        df = pd.DataFrame(beta_data, columns=["Stock", "Beta"])
        df.to_csv(output_file, index=False)  # Save as CSV without index
        print(f"Beta values saved to {output_file}")
    else:
        print("No beta data to save.")

# Example Usage
stocks = ['TATASTEEL', 'INFY', 'SBIN']  # Replace with your list of stocks
index = "^NSEI"  # Nifty 50 Index

# Process the stocks and save the results in a CSV
process_stocks(stocks, index, output_file="beta_values.csv")

from nsepython import *
import numpy as np
import logging
import datetime

# Configure logging
logging.basicConfig(filename="beta_calculation.log", level=logging.ERROR, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

def get_beta_df_maker(symbol, days):
    try:
        end_date = datetime.datetime.now().strftime("%d-%b-%Y") if "NIFTY" in symbol else datetime.datetime.now().strftime("%d-%m-%Y")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%d-%b-%Y" if "NIFTY" in symbol else "%d-%m-%Y")

        if "NIFTY" in symbol:
            df = index_history(symbol, start_date, end_date)
            df["daily_change"] = df["CLOSE"].astype(float).pct_change()
            df = df[['HistoricalDate', 'daily_change']].iloc[1:]
        else:
            df = equity_history(symbol, "EQ", start_date, end_date)
            df["daily_change"] = df["CH_CLOSING_PRICE"].pct_change()
            df = df[['CH_TIMESTAMP', 'daily_change']].iloc[1:]

        return df
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

def get_beta(symbol, days, symbol2="NIFTY 50"):
    # Fetch data for the target symbol and benchmark
    df = get_beta_df_maker(symbol, days)
    df2 = get_beta_df_maker(symbol2, days)
    
    if df is None or df2 is None:
        return None

    # Convert daily change to numpy arrays for performance
    x = np.array(df["daily_change"])
    y = np.array(df2["daily_change"])

    # Calculate covariance and variance using numpy
    covariance = np.cov(x, y)[0][1]
    variance = np.var(y)

    # Calculate beta
    beta = covariance / variance
    return round(beta, 3)

# Dynamic input for days and benchmark
DAYS = 255  # Default to 255 days (1 year of trading days)
BENCHMARK = "NIFTY 50"  # Default benchmark

# Calculate and print beta for all F&O symbols
for symbol in fnolist():
    try:
        beta = get_beta(symbol, DAYS, BENCHMARK)
        if beta is not None:
            print(f"{symbol} : {beta}")
    except Exception as e:
        logging.error(f"Error calculating beta for {symbol}: {e}")

# Calculate beta between NIFTY 50 and NIFTY BANK
symbol = "NIFTY 50"
print(f"{symbol} vs NIFTY BANK: {get_beta(symbol, DAYS, 'NIFTY BANK')}")

symbol = "NIFTY BANK"
print(f"{symbol} vs {BENCHMARK}: {get_beta(symbol, DAYS, BENCHMARK)}")

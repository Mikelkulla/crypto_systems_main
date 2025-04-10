import pandas as pd
from scipy.stats import linregress
from googlesheets_get_functions import get_coin_historical_prices_from_google_sheets, get_coin_list_from_google_sheet
from googlesheets_write_functions import write_to_google_sheet
from typing import List, Tuple, Optional, Union
from config import GOOGLE_PROJECT_CREDENTIALS, SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME, COINS_LIST_SHEET_NAME
import time
def get_beta_using_API(
    token_coin: str,
    spreadsheet_name: str,
    credentials_file: str,
    benchmark: str = "BTC",
    days: int = 365
) -> Tuple[Optional[pd.DataFrame], Optional[float]]:
    """
    Fetches historical price data for a benchmark coin (default BTC) and another token from Google Sheets,
    calculates daily returns, and computes the beta of the token compared to the benchmark.

    Parameters:
        token_coin (str): The token symbol (e.g., "ETH", "AAVE") as it appears in the sheet name (e.g., "ETHUSDT").
        spreadsheet_name (str): Name of the Google Sheet document containing the price data.
        credentials_file (str): Path to the JSON credentials file for Google API authentication.
        benchmark (str): Benchmark coin symbol (default: "BTC" for Bitcoin).
        days (int): Number of past days to consider for beta calculation (default: 365).

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[float]]:
            - DataFrame with prices and returns (columns: 'Date', 'BTC_Price', 'Coin_Price', 'BTC_Return', 'Coin_Return')
            - Beta value (slope of the regression of token returns vs benchmark returns)
            - Returns (None, None) if data fetching or calculation fails.

    Notes:
        - Assumes sheet names are in the format "{coin}USDT" (e.g., "BTCUSDT", "ETHUSDT").
        - Prices in Google Sheets should be in ascending date order with columns: 
          ['Date', 'Open', 'High', 'Low', 'Close', 'Volume (USDT)'].
    """
    print(f'Getting beta for {token_coin} against {benchmark}...')

    # Fetch prices for benchmark (e.g., BTC) and token from Google Sheets
    btc_df = get_coin_historical_prices_from_google_sheets(spreadsheet_name, credentials_file, coin=benchmark)
    if btc_df.empty:
        print(f"Failed to fetch {benchmark} prices from Google Sheets. Aborting beta calculation for {token_coin}.")
        return None, None

    token_df = get_coin_historical_prices_from_google_sheets(spreadsheet_name, credentials_file, coin=token_coin)
    if token_df.empty:
        print(f"Failed to fetch {token_coin} prices from Google Sheets. Aborting beta calculation.")
        return None, None

    # Ensure Date column is in datetime format
    btc_df['Date'] = pd.to_datetime(btc_df['Date'])
    token_df['Date'] = pd.to_datetime(token_df['Date'])

    # Filter to the last 'days' worth of data
    end_date = max(btc_df['Date'].max(), token_df['Date'].max())
    start_date = end_date - pd.Timedelta(days=days)  # days - 1 to include full range
    btc_df = btc_df[btc_df['Date'] >= start_date].copy()
    token_df = token_df[token_df['Date'] >= start_date].copy()

    # Merge dataframes on Date to ensure common dates
    df = pd.merge(
        btc_df[['Date', 'Close']].rename(columns={'Close': 'BTC_Price'}),
        token_df[['Date', 'Close']].rename(columns={'Close': 'Coin_Price'}),
        on='Date',
        how='inner'
    )

    if df.empty:
        print(f"No common dates found between {benchmark} and {token_coin} within the last {days} days. Aborting.")
        return None, None

    # Calculate daily returns
    df['BTC_Return'] = df['BTC_Price'].pct_change()
    df['Coin_Return'] = df['Coin_Price'].pct_change()

    # Drop NaN values (first row will be NaN due to pct_change)
    df.dropna(inplace=True)

    if len(df) < 2:
        print(f"Not enough data points after return calculation for {token_coin} (need at least 2, got {len(df)}).")
        return None, None

    # Compute beta using linear regression (Slope = Beta)
    slope, _, _, _, _ = linregress(df['BTC_Return'], df['Coin_Return'])
    beta = slope

    print(f'Beta calculated for {token_coin} against {benchmark}: {beta}')
    return df, beta

def get_beta(
    benchmark_df: pd.DataFrame,
    token_df: pd.DataFrame,
    benchmark: str = "BTC",
    token_coin: str = None,
    days: int = 365
) -> Tuple[Optional[pd.DataFrame], Optional[float]]:
    """
    Calculates the beta of a token compared to a benchmark using provided price DataFrames.

    Parameters:
        btc_df (pd.DataFrame): DataFrame with benchmark coin (e.g., BTC) historical prices.
                              Expected columns: ['Date', 'Close'] (at minimum).
        token_df (pd.DataFrame): DataFrame with token coin historical prices.
                                Expected columns: ['Date', 'Close'] (at minimum).
        benchmark (str): Benchmark coin symbol (default: "BTC" for Bitcoin).
        token_coin (str): The token symbol (e.g., "ETH", "AAVE"). Optional, used for logging only.
        days (int): Number of past days to consider for beta calculation (default: 365).

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[float]]: 
            - DataFrame with prices and returns (columns: 'Date', 'BTC_Price', 'Coin_Price', 'BTC_Return', 'Coin_Return')
            - Beta value (slope of the regression of token returns vs benchmark returns)
            - Returns (None, None) if data processing or calculation fails.

    Notes:
        - Input DataFrames should have prices in ascending date order with at least 'Date' and 'Close' columns.
    """
    token_name = token_coin if token_coin else "token"
    print(f'Getting beta for {token_name} against {benchmark}...')

    # Check if DataFrames are empty or lack required columns
    if benchmark_df.empty or 'Close' not in benchmark_df.columns or 'Date' not in benchmark_df.columns:
        print(f"Invalid or empty {benchmark} DataFrame. Aborting beta calculation for {token_name}.")
        return None, None

    if token_df.empty or 'Close' not in token_df.columns or 'Date' not in token_df.columns:
        print(f"Invalid or empty {token_name} DataFrame. Aborting beta calculation.")
        return None, None

    # Ensure Date column is in datetime format
    benchmark_df['Date'] = pd.to_datetime(benchmark_df['Date'])
    token_df['Date'] = pd.to_datetime(token_df['Date'])

    # Filter to the last 'days' worth of data
    end_date = max(benchmark_df['Date'].max(), token_df['Date'].max())
    start_date = end_date - pd.Timedelta(days=days)  # days - 1 to include full range
    benchmark_df = benchmark_df[benchmark_df['Date'] >= start_date].copy()
    token_df = token_df[token_df['Date'] >= start_date].copy()

    # Merge dataframes on Date to ensure common dates
    df = pd.merge(
        benchmark_df[['Date', 'Close']].rename(columns={'Close': f'{benchmark}_Price'}),
        token_df[['Date', 'Close']].rename(columns={'Close': f'{token_coin}_Price'}),
        on='Date',
        how='inner'
    )

    if df.empty:
        print(f"No common dates found between {benchmark} and {token_name} within the last {days} days. Aborting.")
        return None, None

    # Calculate daily returns
    df[f'{benchmark}_Return'] = df[f'{benchmark}_Price'].pct_change()
    df[f'{token_coin}_Return'] = df[f'{token_coin}_Price'].pct_change()

    # Drop NaN values (first row will be NaN due to pct_change)
    df.dropna(inplace=True)

    if len(df) < 2:
        print(f"Not enough data points after return calculation for {token_name} (need at least 2, got {len(df)}).")
        return None, None

    # Compute beta using linear regression (Slope = Beta)
    slope, _, _, _, _ = linregress(df[f'{benchmark}_Return'], df[f'{token_coin}_Return'])
    beta = slope

    print(f'Beta calculated for {token_name} against {benchmark}: {beta}')
    return df, beta


# This function acts as a main function and doesn't make the code granular
# Try to make it devided into more peaces and call them in the main function
# Currently this function works as expected but it is not the main goal for me
def import_calculated_beta_to_google_sheet(
    price_spreadsheet_name: str,
    coins_spreadsheet_name: str,
    credentials_file: str,
    output_spreadsheet_name: str,
    range_name: str,
    coins_sheet: str = "Coins",
    output_sheet: str = "BetaScores",
    days: int = 365
) -> None:
    """
    Fetches coin list from Google Sheets, calculates beta for each coin, and writes results to another sheet.

    Args:
        source_spreadsheet_name (str): Name of the Google Sheet with coin list and price data.
        credentials_file (str): Path to the JSON credentials file.
        output_spreadsheet_name (str): Name of the Google Sheet to write beta scores.
        coins_sheet (str): Name of the worksheet with coin list (default: "Coins").
        output_sheet (str): Name of the worksheet to write beta scores (default: "BetaScores").
        days (int): Number of days for beta calculation (default: 365).
    """
    # Step 1: Fetch the coin list
    try:
        coin_list = get_coin_list_from_google_sheet(coins_spreadsheet_name, credentials_file, coins_sheet)
        print(f"Fetched {len(coin_list)} coins from '{coins_spreadsheet_name}' sheet '{coins_sheet}'")
    except Exception as e:
        print(f"Error fetching coin list: {str(e)}")
        return

    # Step 2: Calculate beta for each coin
    beta_results: List[List[Union[str, float]]] = [["Symbol", "Beta"]]  # Header row
    for symbol, exchange, coingecko_id in coin_list:
        # Extract the coin symbol without "USDT" (e.g., "BTCUSDT" -> "BTC")
        if symbol.endswith("USDT"):
            coin = symbol[:-4]  # Remove "USDT" from the end
        else:
            coin = symbol  # Use as is if no "USDT" suffix
            print(f"Warning: Symbol '{symbol}' does not end with 'USDT'. Using '{coin}' for beta calculation.")

        # Calculate beta
        df, beta = get_beta(
            token_coin=coin,
            spreadsheet_name=price_spreadsheet_name,
            credentials_file=credentials_file,
            benchmark="BTC",
            days=days
        )
        # Append result if beta was calculated successfully
        if beta is not None:
            beta_results.append([coin, beta])
        else:
            print(f"Skipping {coin} due to beta calculation failure.")

    # Step 3: Write results to Google Sheet
    if len(beta_results) > 1:  # Ensure there's data beyond the header
        try:
            write_to_google_sheet(
                spreadsheet_name=output_spreadsheet_name,
                credentials_file=credentials_file,
                data=beta_results,
                target_sheet=output_sheet,
                range_name="B4:C" + str(3 + len(beta_results))  # Write to specific range
            )
            print(f"Wrote {len(beta_results) - 1} beta scores to '{output_spreadsheet_name}' sheet '{output_sheet}'")
        except Exception as e:
            print(f"Error writing beta scores to Google Sheet: {str(e)}")
    else:
        print("No beta scores calculated to write.")

# Example usage:
if __name__ == "__main__":
    prices_spreadsheet_name = SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME
    credentials_file = GOOGLE_PROJECT_CREDENTIALS
    coins_spreadsheet_name = 'RSPS LV3'
    output_spreadsheet_name = 'RSPS LV3'
    output_sheet_name = '5.1 - Beta'
    output_range = 'B4:C31'
    coins_sheet_name = COINS_LIST_SHEET_NAME
    import_calculated_beta_to_google_sheet(prices_spreadsheet_name,coins_spreadsheet_name, credentials_file, output_spreadsheet_name, output_range, 'Coins', output_sheet_name, 600)

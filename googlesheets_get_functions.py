# googlesheets_get_functions.py
import pandas as pd
import gspread  # Library to interact with Google Sheets
from oauth2client.service_account import ServiceAccountCredentials  # Handles authentication with Google APIs
from typing import List, Tuple

def get_coin_list_from_google_sheet(
    spreadsheet_name: str, 
    credentials_file: str, 
    coins_sheet: str = "Coins"
) -> List[Tuple[str, str, str]]:
    """
    Fetch a list of coin data (Symbol, Exchange, Coingecko ID) from a Google Sheet worksheet.

    Args:
        spreadsheet_name (str): Name of the Google Sheet document.
        credentials_file (str): Path to the JSON credentials file for authentication.
        coins_sheet (str, optional): Name of the worksheet containing coin data. Defaults to "Coins".

    Returns:
        List[Tuple[str, str, str]]: A list of tuples, each containing:
            - Symbol Binance (str): Binance symbol for the coin (e.g., "BTCUSDT").
            - Exchange (str): Name of the exchange (e.g., "Binance").
            - Coingecko ID (str): CoinGecko identifier (e.g., "bitcoin").
            Rows with all empty values are excluded.

    Raises:
        FileNotFoundError: If the credentials file is not found.
        gspread.exceptions.SpreadsheetNotFound: If the spreadsheet doesn't exist or isn't accessible.
        gspread.exceptions.WorksheetNotFound: If the specified worksheet doesn't exist.
        gspread.exceptions.APIError: If there's an issue with the Google API (e.g., permissions).
        ValueError: If the worksheet has fewer than three columns of data.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    try:
        # Authenticate with the Google Sheets API
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)

        # Open the spreadsheet
        spreadsheet = client.open(spreadsheet_name)

        # Access the specified worksheet
        sheet = spreadsheet.worksheet(coins_sheet)

        # Fetch data from columns A, B, and C (Symbol, Exchange, Coingecko ID), skipping header
        symbol = sheet.col_values(1, value_render_option="FORMATTED_VALUE")[1:]
        exchange = sheet.col_values(2, value_render_option="FORMATTED_VALUE")[1:]
        coingecko_id = sheet.col_values(3, value_render_option="FORMATTED_VALUE")[1:]

        # Check if there are enough columns
        if not symbol or not exchange or not coingecko_id:
            raise ValueError(f"Worksheet '{coins_sheet}' must have at least 3 columns (Symbol, Exchange, Coingecko ID).")

        # Ensure all columns have the same length by padding shorter ones with empty strings
        max_length = max(len(symbol), len(exchange), len(coingecko_id))
        symbol += [""] * (max_length - len(symbol))
        exchange += [""] * (max_length - len(exchange))
        coingecko_id += [""] * (max_length - len(coingecko_id))

        # Combine into a list of tuples and filter out rows where all values are empty
        rows = list(zip(symbol, exchange, coingecko_id))
        return [tuple(str(val).strip() for val in row) for row in rows if any(val.strip() for val in row)]

    except FileNotFoundError:
        raise FileNotFoundError(f"Credentials file not found at: {credentials_file}")
    except gspread.exceptions.SpreadsheetNotFound:
        raise gspread.exceptions.SpreadsheetNotFound(
            f"Spreadsheet '{spreadsheet_name}' not found or inaccessible."
        )
    except gspread.exceptions.WorksheetNotFound:
        raise gspread.exceptions.WorksheetNotFound(
            f"Worksheet '{coins_sheet}' not found in '{spreadsheet_name}'."
        )
    except gspread.exceptions.APIError as e:
        raise gspread.exceptions.APIError(f"API error occurred: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")

def get_coin_historical_prices_from_google_sheets(spreadsheet_name,  credentials_file, coin = 'BTC'):
    """
    Fetch historical price data for a specific coin from a Google Sheet named '{coin}USDT'.
    Reminder! The coin should exists in the google sheet coins list.
    
    Parameters:
    - coin (str): The coin symbol (e.g., "BTC" for Bitcoin)
    - sheet_name (str): The name of the Google Sheet document containing the coin sheets
    - credentials_file (str): Path to the JSON credentials file (default: 'credentials.json')
    
    Returns:
    - pandas.DataFrame: Historical price data with columns:
                        ['Date', 'Open', 'High', 'Low', 'Close', 'Volume (USDT)']
    """
    # Define the scope for Google Sheets and Drive APIs
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Load and authorize credentials
    try:
        # Load and authorize credentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open(spreadsheet_name)
    # except gspread.exceptions.APIError as e:
    #     print(f"API Error: {e} - Check if Drive/Sheets API is enabled and credentials are valid.")
    #     return pd.DataFrame()
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet '{spreadsheet_name}' not found!")
        return pd.DataFrame()
    except FileNotFoundError:
        print(f"Credentials file '{credentials_file}' not found!")
        return pd.DataFrame()
    
    # Construct the coin-specific sheet name
    coin_sheet_name = f"{coin}USDT"
    
    # Try to access the coin-specific sheet
    try:
        sheet = spreadsheet.worksheet(coin_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{coin_sheet_name}' not found in '{spreadsheet_name}'!")
        return pd.DataFrame()  # Return empty DataFrame
    
    # Get all data from the sheet
    data = sheet.get("A:F")
    
    # Check if there's any data
    if not data or len(data) < 2:  # Less than 2 rows means no data beyond header
        print(f"No data found in '{coin_sheet_name}'!")
        return pd.DataFrame()  # Return empty DataFrame
    
    # Create DataFrame directly from data
    df = pd.DataFrame(data[1:], columns=data[0])    

    # Verify expected headers
    expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume (USDT)']
    if list(df.columns) != expected_columns:
        print(f"Warning: Columns in '{coin_sheet_name}' do not match expected format: {expected_columns}")

    # Convert numeric columns to float
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume (USDT)']
    try:
        df[numeric_columns] = df[numeric_columns].astype(float)
    except ValueError as e:
        print(f"Error converting numeric columns to float: {e}")
        # # Optionally, you could drop rows with invalid data here
        # df = df[pd.to_numeric(df['Open'], errors='coerce').notna()]
        # df[numeric_columns] = df[numeric_columns].astype(float)
    
    print(f"Successfully retrieved {len(df)} price entries for {coin}")
    return df


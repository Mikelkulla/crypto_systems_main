import config as conf
import googlesheets_get_functions as gsh_get
import googlesheets_write_functions as gsh_write
from indicators_functions import fdi_adaptive_supertrend, plot_fdi_adaptive_supertrend
from indicators_functions import liquidity_weighted_supertrend, plot_liquidity_weighted_supertrend
import pandas as pd
import numpy as np

def test_fdi_adaptive_supertrend(history_prices_daily_spreadsheet_name, credentials_file):

    df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name, credentials_file, 'BTC')
    print('==================================')
    print(df['Date'])
    # Ensure dates are in datetime format and sorted
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').reset_index(drop=True)
    
    # Run indicator
    result = fdi_adaptive_supertrend(df)
    
    # Find the index where the direction switches to -1 around March 2025
    target_date = pd.to_datetime('2025-03-09')
    target_date_end = pd.to_datetime('2025-03-11')
    mask = (result['date'] >= target_date) & (result['date'] <= target_date_end)
    indices = np.where(mask)[0]
    
    # Print data around the switch
    print("\nData around March 9-10, 2025:")
    for idx in indices:
        print(f"Date: {result['date'][idx]}, Close: {df['Close'].iloc[idx]}, "
              f"High: {df['High'].iloc[idx]}, Low: {df['Low'].iloc[idx]}, "
              f"Supertrend: {result['supertrend'][idx]:.2f}, Direction: {result['direction'][idx]}")
    
    # Print direction changes
    print("\nDirection Changes around March 2025:")
    for i in range(max(0, indices[0] - 5), min(len(result['date']), indices[-1] + 5)):
        if i > 0 and result['direction'][i] != result['direction'][i-1]:
            print(f"Direction change at {result['date'][i]}: {result['direction'][i-1]} -> {result['direction'][i]}")
    
    # Print last 110 days
    print_days = -110
    print("\nLast 110 Days:")
    print("Dates:", result['date'][print_days:])
    print("Supertrend:", result['supertrend'][print_days:])
    print("Direction:", result['direction'][print_days:])
    print("Upper Float:", result['flu'][print_days:])
    print("Lower Float:", result['fld'][print_days:])
    print("Middle Float:", result['flm'][print_days:])
    print("Long Signals:", result['go_long'][print_days:])
    print("Short Signals:", result['go_short'][print_days:])
    
    # Plot
    plot_fdi_adaptive_supertrend(df, result)

def test_liquidity_weighted_supertrend(history_prices_daily_spreadsheet_name, credentials_file):
    """
    Test the Liquidity Weighted Supertrend indicator with Google Sheets data.
    """
    is_token_against_benchmark = True   # Toggle this True for TOKEN/BTC trend or False for TOKEN/USD trend
    if is_token_against_benchmark:
        # Load data for 'RENDER' coin
        toke_df = gsh_get.get_coin_historical_prices_from_google_sheets(
            history_prices_daily_spreadsheet_name, credentials_file, 'SIGMA'
        )
        print('==================================')
        benchmark_df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, 'BTC')
       
        # TOKEN/BTC price series
        # Merge token_df and benchmark_df on Date
        df = pd.DataFrame({
            'Date': toke_df['Date'],
            'Open': toke_df['Open'] / benchmark_df['Open'],
            'High': toke_df['High'] / benchmark_df['High'],
            'Low': toke_df['Low'] / benchmark_df['Low'],
            'Close': toke_df['Close'] / benchmark_df['Close'],
            'Volume (USDT)': toke_df['Volume (USDT)']  # Keep token's volume
        }).dropna()
        print(df)
    else:
        # Load data for 'RENDER' coin
        df = gsh_get.get_coin_historical_prices_from_google_sheets(
            history_prices_daily_spreadsheet_name, credentials_file, 'CRV'
        )
        print('==================================')
        print(df)
    # Ensure dates are in datetime format and sorted
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').reset_index(drop=True)
    
    # Run indicator with default parameters
    result = liquidity_weighted_supertrend(
        df,
        factor=2.5,
        period=75,
        fast_ma_length=46,
        slow_ma_length=65,
        supertrend_type="Smoothed"
    )
    
    # Find the index around March 2025 to analyze direction changes
    target_date = pd.to_datetime('2025-03-09')
    target_date_end = pd.to_datetime('2025-03-11')
    mask = (result['date'] >= target_date) & (result['date'] <= target_date_end)
    indices = np.where(mask)[0]
    
    # Print data around the specified dates
    print("\nData around March 9-10, 2025:")
    for idx in indices:
        print(f"Date: {result['date'][idx]}, Close: {df['Close'].iloc[idx]}, "
              f"High: {df['High'].iloc[idx]}, Low: {df['Low'].iloc[idx]}, "
              f"Supertrend: {result['supertrend'][idx]:.2f}, Direction: {result['direction'][idx]}")
    
    # Print direction changes around March 2025
    print("\nDirection Changes around March 2025:")
    for i in range(max(0, indices[0] - 5), min(len(result['date']), indices[-1] + 5)):
        if i > 0 and result['direction'][i] != result['direction'][i-1]:
            print(f"Direction change at {result['date'][i]}: {result['direction'][i-1]} -> {result['direction'][i]}")
    
    # Print last 110 days
    print_days = -110
    print("\nLast 110 Days:")
    print("Dates:", result['date'][print_days:])
    print("Supertrend:", result['supertrend'][print_days:])
    print("Direction:", result['direction'][print_days:])
    print("Long Signals:", result['go_long'][print_days:])
    print("Short Signals:", result['go_short'][print_days:])
    
    # Plot the results
    plot_liquidity_weighted_supertrend(df, result)

# Test with Google Sheets data
if __name__ == "__main__":
    credentials_file = conf.GOOGLE_PROJECT_CREDENTIALS
    history_prices_daily_spreadsheet_name = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME
    history_prices_daily_spreadsheet_url = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_URL

    # Systems Spreadsheet (keeping same configuration for consistency)
    systems_spreadsheet_name = 'RSPS LV3'
    systems_coins_list_sheet_name = 'Coins'
    systems_beta_targetsheet_name = '5.1 - Beta'
    systems_beta_targetrange_name_BTC = 'B5:C31'
    systems_beta_targetrange_name_TOTAL = 'B4:D31'
    beta_days = 600

    # test_fdi_adaptive_supertrend(history_prices_daily_spreadsheet_name, credentials_file)
    test_liquidity_weighted_supertrend(history_prices_daily_spreadsheet_name, credentials_file)

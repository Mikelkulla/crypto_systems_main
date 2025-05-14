import googlesheets_get_functions as gsh_get
import googlesheets_write_functions as gsh_write
import config as conf
import calculate_beta_scores as cbs
import time
import gspread
import indicators_functions as ind_func
from datetime import datetime
import numpy as np
import pandas as pd
from logging_config import setup_logger

logger = setup_logger(__name__)
    
credentials_file = conf.GOOGLE_PROJECT_CREDENTIALS
# History prices spreadsheet info
history_prices_daily_spreadsheet_name = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME
history_prices_daily_spreadsheet_url = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_URL

# Systems Spreadsheet
systems_spreadsheet_name = 'RSPS LV3'
systems_coins_list_sheet_name = 'Coins'
systems_beta_targetsheet_name = '5.1 - Beta'
systems_beta_targetrange_name_BTC = 'B5:C31'            # Leave C without number to calculate the length based on the coins beta fetched
systems_beta_targetrange_name_TOTAL = 'B4:D31'          # Leave D without number to calculate the length based on the coins beta fetched
beta_days = 600                                         # Time that beta will be calculated

def to_native_type(value):
    if isinstance(value, (np.integer, np.int64, np.int32)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64, np.float32)):
        return float(value)
    else:
        return value

def get_prices(history_prices_daily_spreadsheet_name, credentials_file, coin_list):
        # Append to the tokens list every token name and dataframe of historical prices (fetched from function get_coin_historical_prices_from_google_sheets)
    retries = 5
    tokens_prices_list = []
    skipped_coins = []
    for coin in coin_list:
        coin_binance_name = coin[0]
        for attempt in range(retries):
            try:   
                tokens_prices_list.append([coin_binance_name, gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, coin_binance_name)])
                break
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    delay = 6 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    logger.warning(f"429 Too Many Requests for {coin_binance_name} (attempt {attempt + 1}/{retries}): {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Non-429 API error for {coin_binance_name}: {e}")
                    skipped_coins.append(coin_binance_name)
                    break

            except Exception as e:
                logger.error(f"Unexpected error for {coin_binance_name}: {e}")
                skipped_coins.append(coin_binance_name)
                break
            if attempt == retries - 1:
                logger.warning(f"Max retries ({retries}) reached for {coin_binance_name}. Skipping.")
                skipped_coins.append(coin_binance_name)
                break
    return tokens_prices_list, skipped_coins    


def main_beta():
    """
    Main function to calculate beta scores without overwellming the API Sheets
    - The main plan is to also calcuate beta agains TOTAL too, and pass all the beta scores to the RSPS LV3 sheet Symbol, BTC Beta, TOTAL Beta
    """
    credentials_file = conf.GOOGLE_PROJECT_CREDENTIALS
    # History prices spreadsheet info
    history_prices_daily_spreadsheet_name = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME
    history_prices_daily_spreadsheet_url = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_URL


    # Systems Spreadsheet
    systems_spreadsheet_name = 'RSPS LV3'
    systems_coins_list_sheet_name = 'Coins'
    systems_beta_targetsheet_name = '5.1 - Beta'
    systems_token_usdt_sheet_name = '5 - Trash Selection Table'
    systems_token_usdt_range_name = 'B10:G'
    systems_beta_targetrange_name_BTC = 'B4:C'            # Leave C without number to calculate the length based on the coins beta fetched
    systems_beta_targetrange_name_TOTAL = 'B4:D'          # Leave D without number to calculate the length based on the coins beta fetched
    beta_days = 600                                         # Time that beta will be calculated

    # Fetching crypto coins list
    try:
        coin_list = gsh_get.get_coin_list_from_google_sheet(systems_spreadsheet_name,credentials_file,systems_coins_list_sheet_name)
        logger.info(f"Fetched {len(coin_list)} coins from '{systems_spreadsheet_name}' sheet '{systems_coins_list_sheet_name}'")
    except Exception as e:
        logger.error(f"Error fetching coin list: {str(e)}")
        return
    benchmark_beta_name = 'BTC'
    benchmark_trend_name = 'BTC'
    benchmark_trend_name_2 = 'SOL'
    benchmark_trend_name_3 = 'SUI'
    benchmark_trend_name_4 = 'ETH'
    if benchmark_beta_name == benchmark_trend_name:
        # Fetch Benchmark price history 
        benchmark_df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_beta_name)
        benchmark_trend_df = benchmark_df 
    else:
        benchmark_df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_beta_name)
        benchmark_trend_df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_trend_name)
        benchmark_trend_df['Date'] = pd.to_datetime(benchmark_trend_df['Date'], errors='coerce')

    benchmark_trend_df_2 = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_trend_name_2)
    benchmark_trend_df_2['Date'] = pd.to_datetime(benchmark_trend_df_2['Date'], errors='coerce')
    benchmark_trend_df_3 = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_trend_name_3)
    benchmark_trend_df_3['Date'] = pd.to_datetime(benchmark_trend_df_3['Date'], errors='coerce')
    benchmark_trend_df_4 = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_trend_name_4)
    benchmark_trend_df_4['Date'] = pd.to_datetime(benchmark_trend_df_4['Date'], errors='coerce')
    # Fetching Tokens prices
    tokens_prices_list, skipped_coins = get_prices(history_prices_daily_spreadsheet_name, credentials_file, coin_list)
    if skipped_coins:
        logger.warning(f"Skipped coins due to errors: {skipped_coins}")
    print(tokens_prices_list)


    # Create timestamp
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    header_row = ["Updated", current_time]
    
    # Calculate beta for each token and append to a list of [Token name, [df, beta score]]
    logger.info("Calculating beta scores...")
    beta_scores_list = []
    for token_name, token_df in tokens_prices_list:
        df, calculated_beta = cbs.get_beta(benchmark_df,token_df, benchmark_beta_name, token_name, beta_days)
        beta_scores_list.append([token_name, calculated_beta])

    # Prepend the header row
    logger.info("Writing beta scores to Google Sheets...")
    beta_scores_list.insert(0, header_row)
    gsh_write.write_to_google_sheet(
        systems_spreadsheet_name, 
        credentials_file, 
        beta_scores_list, 
        systems_beta_targetsheet_name, 
        systems_beta_targetrange_name_BTC
        )
    
    # Calculate trend indicators for TOKEN/USDT and TOKEN/BTC
    logger.info("Calculating trend indicators for TOKEN/USDT and TOKEN/BTC...")
    token_trend_scores_list = []
    for token_name, token_df in tokens_prices_list:
        logger.info(f'Calculating trends for {token_name}')

        # TOKEN/USDT trend (fdi_adaptive_supertrend)
        usdt_scores = ind_func.fdi_adaptive_supertrend(token_df)
        usdt_last_score = usdt_scores['direction'][-1] if len(usdt_scores['direction']) > 0 else None
        
        # =======================================================================================
        
        # TOKEN/BTC price series
        # Merge token_df and benchmark_trend_df on Date
        btc_merged_df = pd.merge(
            token_df, benchmark_trend_df, on='Date', how='inner', suffixes=('_token', '_btc')
        )
        btc_df = pd.DataFrame({
            'Date': btc_merged_df['Date'],
            'Open': btc_merged_df['Open_token'] / btc_merged_df['Open_btc'],
            'High': btc_merged_df['High_token'] / btc_merged_df['High_btc'],
            'Low': btc_merged_df['Low_token'] / btc_merged_df['Low_btc'],
            'Close': btc_merged_df['Close_token'] / btc_merged_df['Close_btc'],
            'Volume (USDT)': btc_merged_df['Volume (USDT)_token']
        })

        # Check for missing values
        # print("Missing values in btc_df:\n", btc_df.isna().sum())
        btc_df = btc_df.dropna()

        # TOKEN/BTC trend (liquidity_weighted_supertrend)
        btc_scores = ind_func.liquidity_weighted_supertrend(btc_df)
        btc_last_score = btc_scores['direction'][-1] if len(btc_scores['direction']) > 0 else None

        # =======================================================================================

        # TOKEN/SOL price series
        # Merge token_df and benchmark_trend_df_2 on Date
        sol_merged_df = pd.merge(
            token_df, benchmark_trend_df_2, on='Date', how='inner', suffixes=('_token', '_sol')
        )
        token_sol_df = pd.DataFrame({
            'Date': sol_merged_df['Date'],
            'Open': sol_merged_df['Open_token'] / sol_merged_df['Open_sol'],
            'High': sol_merged_df['High_token'] / sol_merged_df['High_sol'],
            'Low': sol_merged_df['Low_token'] / sol_merged_df['Low_sol'],
            'Close': sol_merged_df['Close_token'] / sol_merged_df['Close_sol'],
            'Volume (USDT)': sol_merged_df['Volume (USDT)_token']
        })

        # Check for missing values
        # print("Missing values in token_sol_df:\n", token_sol_df.isna().sum())
        token_sol_df = token_sol_df.dropna()

        # TOKEN/SOL trend (liquidity_weighted_supertrend)
        token_sol_scores = ind_func.liquidity_weighted_supertrend(token_sol_df)
        token_sol_last_score = token_sol_scores['direction'][-1] if len(token_sol_scores['direction']) > 0 else None

        # =======================================================================================

        # TOKEN/SUI price series
        # Merge token_df and benchmark_trend_df_3 on Date
        sui_merged_df = pd.merge(
            token_df, benchmark_trend_df_3, on='Date', how='inner', suffixes=('_token', '_sui')
        )
        token_sui_df = pd.DataFrame({
            'Date': sui_merged_df['Date'],
            'Open': sui_merged_df['Open_token'] / sui_merged_df['Open_sui'],
            'High': sui_merged_df['High_token'] / sui_merged_df['High_sui'],
            'Low': sui_merged_df['Low_token'] / sui_merged_df['Low_sui'],
            'Close': sui_merged_df['Close_token'] / sui_merged_df['Close_sui'],
            'Volume (USDT)': sui_merged_df['Volume (USDT)_token']
        })

        # Check for missing values
        # print("Missing values in token_sui_df:\n", token_sui_df.isna().sum())
        token_sui_df = token_sui_df.dropna()

        # TOKEN/SUI trend (liquidity_weighted_supertrend)
        token_sui_scores = ind_func.liquidity_weighted_supertrend(token_sui_df)
        token_sui_last_score = token_sui_scores['direction'][-1] if len(token_sui_scores['direction']) > 0 else None

        # =======================================================================================

        # TOKEN/ETH price series
        # Merge token_df and benchmark_trend_df_4 on Date
        eth_merged_df = pd.merge(
            token_df, benchmark_trend_df_4, on='Date', how='inner', suffixes=('_token', '_eth')
        )
        token_eth_df = pd.DataFrame({
            'Date': eth_merged_df['Date'],
            'Open': eth_merged_df['Open_token'] / eth_merged_df['Open_eth'],
            'High': eth_merged_df['High_token'] / eth_merged_df['High_eth'],
            'Low': eth_merged_df['Low_token'] / eth_merged_df['Low_eth'],
            'Close': eth_merged_df['Close_token'] / eth_merged_df['Close_eth'],
            'Volume (USDT)': eth_merged_df['Volume (USDT)_token']
        })

        # Check for missing values
        # print("Missing values in token_eth_df:\n", token_eth_df.isna().sum())
        token_eth_df = token_eth_df.dropna()

        # TOKEN/ETH trend (liquidity_weighted_supertrend)
        token_eth_scores = ind_func.fdi_adaptive_supertrend(token_eth_df)
        token_eth_last_score = token_eth_scores['direction'][-1] if len(token_eth_scores['direction']) > 0 else None

        # Append [token_name, usdt_score, btc_score, sol_score, sui_score, eth_score,]
        token_trend_scores_list.append([token_name, to_native_type(usdt_last_score), to_native_type(btc_last_score), to_native_type(token_sol_last_score), to_native_type(token_sui_last_score), to_native_type(token_eth_last_score)])


    print(token_trend_scores_list)
    # Prepend the header row
    token_trend_scores_list.insert(0, header_row)
    logger.info("Writing trend scores to Google Sheets...")

    gsh_write.write_to_google_sheet(
        systems_spreadsheet_name,
        credentials_file,
        token_trend_scores_list,
        systems_token_usdt_sheet_name,
        systems_token_usdt_range_name
    )
    return token_trend_scores_list

if __name__ == "__main__":
    main_beta()
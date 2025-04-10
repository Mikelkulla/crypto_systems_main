import googlesheets_get_functions as gsh_get
import googlesheets_write_functions as gsh_write
import config as conf
import calculate_beta_scores as cbs
import time
import gspread

# prices = gsh_get_f.get_coin_historical_prices_from_google_sheets(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME, credentials_file=conf.GOOGLE_PROJECT_CREDENTIALS)
# print(prices)

# coins = gsh_get_f.get_coin_list_from_google_sheet(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME, conf.GOOGLE_PROJECT_CREDENTIALS, conf.COINS_LIST_SHEET_NAME)
# for coin in coins:
#     print(coin, '\n')
# gsh_write_f.write_to_google_sheet(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME,conf.GOOGLE_PROJECT_CREDENTIALS,coins,  0)


def main():
    """
    Main function to calculate beta scores without overwellming the API Sheets
    - Needs to implement logic for 429 error as i received it again. Retry after some time
    - Needs to implement write to google sheet as it just calculates but doesn't write
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
    systems_beta_targetrange_name_BTC = 'B5:C31'            # Leave C without number to calculate the length based on the coins beta fetched
    systems_beta_targetrange_name_TOTAL = 'B4:D31'          # Leave D without number to calculate the length based on the coins beta fetched
    beta_days = 600                                         # Time that beta will be calculated

    # Fetching crypto coins list
    try:
        coin_list = gsh_get.get_coin_list_from_google_sheet(systems_spreadsheet_name,credentials_file,systems_coins_list_sheet_name)
        print(f"Fetched {len(coin_list)} coins from '{systems_spreadsheet_name}' sheet '{systems_coins_list_sheet_name}'")
    except Exception as e:
        print(f"Error fetching coin list: {str(e)}")
        return
    benchmark_name = 'BTC'
    # Fetch Benchmark price history 
    benchmark_df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name,credentials_file, benchmark_name)

    # Append to the tokens list every token name and dataframe of historical prices (fetched from function get_coin_historical_prices_from_google_sheets)
    retries = 3
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
                    delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    print(f"429 Too Many Requests for {coin_binance_name} (attempt {attempt + 1}/{retries}): {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"Non-429 API error for {coin_binance_name}: {e}")
                    skipped_coins.append(coin_binance_name)
                    break

            except Exception as e:
                print(f"Unexpected error for {coin_binance_name}: {e}")
                skipped_coins.append(coin_binance_name)
                break
            if attempt == retries - 1:
                print(f"Max retries ({retries}) reached for {coin_binance_name}. Skipping.")
                skipped_coins.append(coin_binance_name)
                break
        
        print(coin[0])
    if skipped_coins:
        print(f"Skipped coins due to errors: {skipped_coins}")
    print(tokens_prices_list)

    # Calculate beta for each token and append to a list of [Token name, [df, beta score]]
    beta_scores_list = []
    for token_name, token_df in tokens_prices_list:
        df, calculated_beta = cbs.get_beta(benchmark_df,token_df, benchmark_name, token_name, beta_days)
        beta_scores_list.append([token_name, calculated_beta])

    gsh_write.write_to_google_sheet(systems_spreadsheet_name, credentials_file, beta_scores_list, systems_beta_targetsheet_name, systems_beta_targetrange_name_BTC)
    # Just prints the results in the console. For debugging purpose
    # From here on I will apply the logic to write to goole sheets (RSPS LV3 Beta sheet)
    for token_name, calculated_beta in beta_scores_list:
        print(token_name,calculated_beta)

if __name__ == "__main__":
    main()
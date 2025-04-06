import googlesheets_get_functions as gsh_get_f
import googlesheets_write_functions as gsh_write_f
import config as conf

# prices = gsh_get_f.get_coin_historical_prices_from_google_sheets(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME, credentials_file=conf.GOOGLE_PROJECT_CREDENTIALS)
# print(prices)

# coins = gsh_get_f.get_coin_list_from_google_sheet(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME, conf.GOOGLE_PROJECT_CREDENTIALS, conf.COINS_LIST_SHEET_NAME)
# for coin in coins:
#     print(coin, '\n')
# gsh_write_f.write_to_google_sheet(conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME,conf.GOOGLE_PROJECT_CREDENTIALS,coins,  0)

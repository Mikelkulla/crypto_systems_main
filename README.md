# Crypto Systems Main Project
## Getting data from Google sheets documents (fetched before)
- Crypto prices
- Coins List
## Writing to Google sheets documents 
- Writing directly to a sheet (create new if don't exists) when range specified
- Append to the sprecified sheet if no range is specified

## Last done
Created get_beta() with fetching prices from googlesheets
Created calculate_beta_scores.py file
Created import_calculated_beta_to_google_sheet()
- Check where the beta scores are passed
- Try to add TOTAL with the py library
- Try to reduce the requests sent to google sheets for prices in get beta where btc is called every time
- Make calculate_beta_scores.py more automatic and check variables, function arguments, returns etc.
- Add logic to handle 429 error to many request by waiting and retrying
The logic in calculate_beta_scores.py file should be added in a main script that will update the whole systems 

After making this changes i need to push to git to refresh the codes there
Check ToDo sheet in RSPS LV3 spreadsheet for todos extra.

# 11/04/2025
Why does the direction in the indicaotr is in revers? -1 for uptrend and 1 for downtrend
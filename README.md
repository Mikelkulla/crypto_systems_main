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

# Building the Container. 
- https://grok.com/chat/1f1d763f-0809-4ccb-8dfe-55f0d4a1eb7b#:~:text=share%20its%20contents.-,Step%205%3A%20Deploy%20to%20Google%20Cloud%20Run,-Assuming%20Google%20Cloud

Built the my-app container and made it active and running on port 8080.
Left at step 5 from grok.com

# How to push container to GitHub and Pull to Google Cloud Run
[ChatGPT Link to Chat](https://chatgpt.com/share/67fd24eb-da74-8003-8e93-75cd41d836c5)


# Docker Contaier on Google Cloud Run
URL: 
Used this chat to deploy to google Artifact Registry and to Google Cloud Run:
    - https://chatgpt.com/share/67fd809a-62e8-8003-a954-1b95a0fc1cf6
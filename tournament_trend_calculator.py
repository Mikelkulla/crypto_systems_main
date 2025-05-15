import googlesheets_get_functions as gsh_get
import googlesheets_write_functions as gsh_write
import config as conf
import indicators_functions as ind_func
import pandas as pd
from logging_config import setup_logger
import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = setup_logger(__name__)

credentials_file = conf.GOOGLE_PROJECT_CREDENTIALS
history_prices_daily_spreadsheet_name = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME

def get_token_list(spreadsheet_name, sheet_name, range_name):
    """Fetch token names from the specified range in Google Sheets."""
    try:
        token_data = gsh_get.get_range_from_google_sheet(spreadsheet_name, credentials_file, sheet_name, range_name)
        # Flatten the list and remove empty cells
        tokens = [token for sublist in token_data for token in sublist if token]
        logger.info(f"Fetched tokens: {tokens}")
        return tokens
    except Exception as e:
        logger.error(f"Error fetching token list from {sheet_name} range {range_name}: {str(e)}")
        raise

def calculate_pairwise_trends(token_list):
    """Calculate trends for each pair of tokens and build a tournament matrix."""
    # Fetch historical prices for all tokens
    token_dfs = {}
    for token in token_list:
        try:
            df = gsh_get.get_coin_historical_prices_from_google_sheets(
                history_prices_daily_spreadsheet_name, credentials_file, token
            )
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date'])
            token_dfs[token] = df
        except Exception as e:
            logger.warning(f"Failed to fetch data for {token}: {str(e)}")
            token_dfs[token] = None

    # Initialize n x n matrix for n tokens
    n = len(token_list)
    matrix = [[0 for _ in range(n)] for _ in range(n)]
    
    for i, token1 in enumerate(token_list):
        matrix[i][i] = 'xx'  # Diagonal
        for j, token2 in enumerate(token_list):
            if i < j:  # Upper triangle (to avoid duplicate calculations)
                if token1 in token_dfs and token2 in token_dfs and token_dfs[token1] is not None and token_dfs[token2] is not None:
                    merged_df = pd.merge(
                        token_dfs[token1], token_dfs[token2], on='Date', how='inner', suffixes=('_t1', '_t2')
                    )
                    if not merged_df.empty:
                        trend_df = pd.DataFrame({
                            'Date': merged_df['Date'],
                            'Open': merged_df['Open_t1'] / merged_df['Open_t2'],
                            'High': merged_df['High_t1'] / merged_df['High_t2'],
                            'Low': merged_df['Low_t1'] / merged_df['Low_t2'],
                            'Close': merged_df['Close_t1'] / merged_df['Close_t2'],
                            'Volume (USDT)': merged_df['Volume (USDT)_t1']
                        })
                        trend_df = trend_df.dropna()
                        if not trend_df.empty:
                            trend_scores = ind_func.fdi_adaptive_supertrend(trend_df)
                            last_score = trend_scores['direction'][-1] if len(trend_scores['direction']) > 0 else None
                            if last_score is not None and last_score > 0:  # Assuming positive trend if direction > 0
                                matrix[i][j] = 1
                                matrix[j][i] = 0  # Inverse for the other direction
                            else:
                                matrix[i][j] = 0
                                matrix[j][i] = 1
                        else:
                            logger.warning(f"No valid data for {token1}/{token2} after merging")
                            matrix[i][j] = 0
                            matrix[j][i] = 0
                    else:
                        logger.warning(f"No overlapping dates for {token1}/{token2}")
                        matrix[i][j] = 0
                        matrix[j][i] = 0
                else:
                    logger.warning(f"Missing data for {token1} or {token2}")
                    matrix[i][j] = 0
                    matrix[j][i] = 0

    return matrix, token_list

def write_matrix_to_google_sheets(matrix, tokens, spreadsheet_name, sheet_name, range_name):
    """Write the tournament matrix to Google Sheets starting at B1, ensuring numeric formatting."""
    # Prepare data with tokens as header and matrix rows
    data = [tokens]  # Header row
    data.extend(matrix)  # Matrix rows (keep 0 and 1 as integers)

    # Write to Google Sheets using the provided function
    logger.info(f"Writing tournament matrix to {sheet_name} at range {range_name}...")
    gsh_write.write_to_google_sheet(spreadsheet_name, credentials_file, data, target_sheet=sheet_name, range_name=range_name)

    # Apply numeric formatting to the data range (excluding header row)
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    client = gspread.authorize(creds)
    sheet = client.open(spreadsheet_name).worksheet(sheet_name)
    
    # Determine the data range (start from B2 to the end of the matrix)
    n = len(tokens)
    data_range = f"B2:{chr(ord('B') + n - 1)}{n + 1}"  # e.g., B2:I9 for 8 tokens
    cell_list = sheet.range(data_range)
    for cell in cell_list:
        if cell.value in ['0', '1']:  # Check for numeric string values
            cell.value = float(cell.value)  # Convert to numeric
    sheet.update_cells(cell_list)
    logger.info(f"Applied numeric formatting to {data_range} in {sheet_name}.")

def main():
    """Main function to run the tournament trend calculation."""
    spreadsheet_name = 'RSPS LV3'
    sheet_name = 'Tournament Matrix'
    token_range = 'A2:A'  # Range to fetch token names

    try:
        # Fetch token list from A1:A
        token_list = get_token_list(spreadsheet_name, sheet_name, token_range)
        if not token_list:
            raise ValueError("No tokens found in the specified range A1:A")

        # Calculate pairwise trends
        matrix, tokens = calculate_pairwise_trends(token_list)

        # Determine output range dynamically (B1 to n x n matrix)
        n = len(tokens)
        end_column = chr(ord('B') + n - 1)  # B + (n-1) for columns (e.g., B to I for 8 tokens)
        end_row = n + 1  # 1 for header + n rows
        output_range = f"B1:{end_column}{end_row}"  # e.g., B1:I9 for 8 tokens (including header)

        # Write matrix to Google Sheets
        write_matrix_to_google_sheets(matrix, tokens, spreadsheet_name, sheet_name, output_range)

    except Exception as e:
        logger.error(f"Error in tournament trend calculation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
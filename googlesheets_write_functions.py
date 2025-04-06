# googlesheets_write_functions.py
import pandas as pd
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from typing import List, Union, Optional

def write_to_google_sheet(
    spreadsheet_name: str,
    credentials_file: str,
    data: List[List[Union[str, int, float]]],
    target_sheet: Optional[Union[str, int]] = None,
    range_name: Optional[str] = None,
    
) -> None:
    """
    Write data to a specific sheet and range in a Google Sheet document. Creates the spreadsheet or worksheet if it doesn't exist.

    Args:
        spreadsheet_name (str): Name of the Google Sheet document (e.g., "Beta Scores First Test").
        data (List[List[Union[str, int, float]]]): Data to write, where each inner list is a row (e.g., [["2023-01-01", 100, 105]]).
        target_sheet (Optional[Union[str, int]]): Name (e.g., "Sheet2") or 0-based index (e.g., 1) of the target sheet.
                                                  Defaults to None, which uses the first sheet.
        range_name (Optional[str]): A1 notation range to write to (e.g., "A1:C2"). If None, appends data as new rows.
        credentials_file (str): Path to the JSON credentials file for Google API authentication. Defaults to 'credentials.json'.

    Raises:
        FileNotFoundError: If the credentials file is missing or invalid.
        gspread.exceptions.APIError: If there's an authentication or API quota issue.
        ValueError: If data is empty or range_name doesn't match data dimensions.

    Returns:
        None: Prints success message upon completion.
    """
    # Define API scopes for Sheets and Drive access
    # Using Sheets v4 API for modern compatibility; Drive scope allows spreadsheet creation
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    try:
        # Load and authenticate with service account credentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
    except FileNotFoundError:
        raise FileNotFoundError(f"Credentials file '{credentials_file}' not found or inaccessible.")
    except Exception as e:
        raise gspread.exceptions.APIError(f"Authentication failed: {str(e)}")

    try:
        # Open the existing spreadsheet by name
        spreadsheet = client.open(spreadsheet_name)
    except SpreadsheetNotFound:
        # If spreadsheet doesn't exist, create it and share with a user for access
        spreadsheet = client.create(spreadsheet_name)
        spreadsheet.share('mikel.kulla84@gmail.com', perm_type='user', role='writer')
        spreadsheet.share('mikelccai@gmail.com', perm_type='user', role='writer')
        print(f"Created new spreadsheet: '{spreadsheet_name}' and shared with 'mikel.kulla84@gmail.com' and 'mikelccai@gmail.com'.")
    # Determine which specific sheet (tab) within the document to write to
    if target_sheet is None:
        # If no specific sheet is provided, default to the first sheet in the document
        sheet = spreadsheet.sheet1
    elif isinstance(target_sheet, int):
        # If target_sheet is an integer, treat it as a 0-based index
        # get_worksheet(0) gets the first sheet, get_worksheet(1) gets the second, etc.
        sheet = spreadsheet.get_worksheet(target_sheet)
    else:
        try:
            sheet = spreadsheet.worksheet(target_sheet)
        except gspread.exceptions.WorksheetNotFound:
            # If the specified worksheet doesn't exist, create it
            sheet = spreadsheet.add_worksheet(title=target_sheet, rows=10000, cols=20)
            print(f"Created new worksheet: {target_sheet} in {spreadsheet_name}")

    # Check if a specific range is provided
    if range_name:
        # Write data to the specified range (e.g., "A1:C2")
        # The data must match the size of the range (rows and columns)
        # For example, "A1:C2" expects 2 rows and 3 columns of data
        sheet.update(range_name, data)
        sheet.format(f"A1:A{len(data)}", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}})
        print(f"Data written successfully to '{sheet.title}' in {spreadsheet_name} at range {range_name}!")
    else:
        # If no range is specified, append the data as new rows
        # This adds the data starting at the first empty row in the sheet
        sheet.append_rows(data)
        sheet.format(f"A1:A{len(data)}", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}})
        print(f"Data appended successfully to '{sheet.title}' in {spreadsheet_name}!")
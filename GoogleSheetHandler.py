import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import pandas as pd
import json
import os

# Load configuration
def load_config():
    """Load configuration from config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Could not load config.json: {e}. Using defaults.")
        return {
            "sheet_name": "Our Listings",
            "store_url_column": "Store URL",
            "audit_links_column": "Audit Links",
            "store_url_column_index": 2,
            "audit_links_column_index": 3
        }

def connect_to_sheet(credentials_path, sheet_name=None, worksheet_name=None):
    """
    Connects to Google Sheets using credentials.json.
    sheet_name: Name of the Google Sheets file (e.g., "Amazesst Master Leads Sheet")
    worksheet_name: Name of the specific tab/worksheet within the file (e.g., "Amazesst Master Leads Sheet")
    Returns the Worksheet object.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Use config sheet name if not provided
    if not sheet_name:
        config = load_config()
        sheet_name = config.get("sheet_name", "Our Listings")
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        
        # Open the Google Sheets file
        spreadsheet = client.open(sheet_name)
        
        # Select the specific worksheet/tab
        # If worksheet_name is provided, use it; otherwise use the same name as sheet_name
        if worksheet_name:
            sheet = spreadsheet.worksheet(worksheet_name)
        else:
            # Try to find worksheet with same name as the file, otherwise use first sheet
            try:
                sheet = spreadsheet.worksheet(sheet_name)
            except:
                logging.warning(f"Worksheet '{sheet_name}' not found. Using first sheet.")
                sheet = spreadsheet.sheet1
        
        logging.info(f"Successfully connected to Google Sheet: {sheet_name}, Worksheet: {sheet.title}")
        return sheet
    except Exception as e:
        logging.error(f"Error connecting to Google Sheet: {e}")
        return None

def get_sheet_data(sheet):
    """
    Reads all records from the sheet and returns as a Pandas DataFrame.
    Handles duplicate/empty headers by reading raw values.
    """
    try:
        # Get all values from sheet
        all_values = sheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            logging.warning("Sheet is empty or has no data rows")
            return pd.DataFrame()
        
        # First row is headers
        headers = all_values[0]
        data_rows = all_values[1:]
        
        # Handle duplicate/empty headers by adding suffixes
        seen = {}
        unique_headers = []
        for header in headers:
            if header == '' or header in seen:
                # Generate unique name for empty/duplicate headers
                base = header if header else 'Unnamed'
                count = seen.get(base, 0)
                seen[base] = count + 1
                unique_headers.append(f"{base}_{count}" if count > 0 else base)
            else:
                seen[header] = 0
                unique_headers.append(header)
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=unique_headers)
        logging.info(f"Successfully read {len(df)} rows from sheet")
        return df
        
    except Exception as e:
        logging.error(f"Error reading sheet data: {e}")
        return pd.DataFrame()

def update_audit_link(sheet, row_number, audit_url):
    """
    Updates the 'Audit Links' column for a specific row.
    row_number: Sheet row number (1-indexed, e.g., 2 for first data row)
    audit_url: The URL to write
    """
    try:
        # Get column index from config
        config = load_config()
        audit_column_index = config.get("audit_links_column_index", 3)
        
        sheet.update_cell(row_number, audit_column_index, audit_url)
        logging.info(f"Updated Row {row_number}, Column {audit_column_index} with: {audit_url}")
        return True
    except Exception as e:
        logging.error(f"Error updating cell: {e}")
        return False

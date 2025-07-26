
from langchain.tools import tool
from langchain.pydantic_v1 import BaseModel, Field
from typing import Optional
import pandas as pd
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

# Configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
credentials = service_account.Credentials.from_service_account_file(
    'service_account.json', scopes=SCOPES)


class SheetsService:
    """Direct Google Sheets operations"""
    
    def __init__(self):
        self.service = build('sheets', 'v4', credentials=credentials)
    
    def read_sheet(self, sheet_name: str):
        """Read data from a Google Sheet"""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=sheet_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return pd.DataFrame()
            if len(values) == 1:
                return pd.DataFrame(columns=values[0])
            
            df = pd.DataFrame(values[1:], columns=values[0])
            # Auto-convert numeric columns
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            
            return df
        except Exception as e:
            raise Exception(f"Error reading sheet: {e}")
    
    def create_sheet(self, sheet_name: str):
        """Create a new sheet if it doesn't exist"""
        try:
            metadata = self.service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            existing_sheets = [sheet['properties']['title'] for sheet in metadata.get('sheets', [])]
            
            if sheet_name not in existing_sheets:
                requests = [{
                    'addSheet': {
                        'properties': {'title': sheet_name}
                    }
                }]
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={'requests': requests}
                ).execute()
            
            return True
        except Exception as e:
            raise Exception(f"Error creating sheet: {e}")
    
    def write_to_sheet(self, df, sheet_name: str, start_cell: str = "A1"):
        """Write DataFrame to Google Sheet"""
        try:
            # Create sheet if it doesn't exist
            self.create_sheet(sheet_name)
            
            if df.empty:
                # Write just headers for empty DataFrame
                if hasattr(df, 'columns') and len(df.columns) > 0:
                    values = [df.columns.tolist()]
                    result = self.service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{sheet_name}!{start_cell}",
                        valueInputOption='RAW',
                        body={'values': values}
                    ).execute()
                    return f"No data found matching criteria. Created sheet with headers only."
                else:
                    # Completely empty - write a message
                    values = [["No data found"]]
                    result = self.service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"{sheet_name}!{start_cell}",
                        valueInputOption='RAW',
                        body={'values': values}
                    ).execute()
                    return f"No data found matching criteria."
            
            # Prepare data for non-empty DataFrame
            # Ensure all rows have the same number of columns as headers
            headers = df.columns.tolist()
            data_rows = []
            
            for _, row in df.iterrows():
                row_data = row.tolist()
                # Pad or truncate row to match header length
                if len(row_data) < len(headers):
                    row_data.extend([''] * (len(headers) - len(row_data)))
                elif len(row_data) > len(headers):
                    row_data = row_data[:len(headers)]
                data_rows.append(row_data)
            
            values = [headers] + data_rows
            
            # Write to sheet
            result = self.service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!{start_cell}",
                valueInputOption='RAW',
                body={'values': values}
            ).execute()
            
            return f"Successfully wrote {result.get('updatedCells', 0)} cells to {sheet_name}!{start_cell}"
        except Exception as e:
            raise Exception(f"Error writing to sheet: {e}")
        

    

# Create global service instance
sheets_service = SheetsService()

def get_sheet_names():
    """Get list of sheet names from the spreadsheet"""
    try:
        service = build('sheets', 'v4', credentials=credentials)
        metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = metadata.get('sheets', '')
        return [s['properties']['title'] for s in sheets]
    except Exception as e:
        raise Exception(f"Error getting sheet names: {e}")

class FilterDataInput(BaseModel):
    """Input for filter_sheets_data tool"""
    condition: str = Field(description="Pandas query condition to filter data (e.g., 'salary > 50000')")
    target_sheet: Optional[str] = Field(default=None, description="Name of target sheet to save filtered data")

class AggregateDataInput(BaseModel):
    """Input for aggregate_sheets_data tool"""
    group_by: str = Field(description="Column name to group by")
    agg_column: str = Field(description="Column name to aggregate")
    agg_method: str = Field(description="Aggregation method: sum, mean, count, min, max")
    target_sheet: Optional[str] = Field(default=None, description="Name of target sheet to save aggregated data")

class PivotTableInput(BaseModel):
    """Input for create_pivot_table tool"""
    index_col: str = Field(description="Column to use as pivot table rows")
    columns_col: str = Field(description="Column to use as pivot table columns")
    values_col: str = Field(description="Column to use as pivot table values")
    agg_func: str = Field(default="sum", description="Aggregation function: sum, mean, count, min, max")
    target_sheet: Optional[str] = Field(default=None, description="Name of target sheet to save pivot table")

class SortDataInput(BaseModel):
    """Input for sort_sheets_data tool"""
    sort_column: str = Field(description="Column name to sort by")
    ascending: bool = Field(default=True, description="Sort in ascending order if True, descending if False")
    target_sheet: Optional[str] = Field(default=None, description="Name of target sheet to save sorted data")

class AddColumnInput(BaseModel):
    """Input for add_column_to_sheet tool"""
    column_name: str = Field(description="Name of the new column to add")
    formula: Optional[str] = Field(default=None, description="Pandas formula to calculate column values (e.g., 'salary * 0.1')")
    default_value: str = Field(default="", description="Default value if no formula provided")
    position: Optional[int] = Field(default=None, description="Position to insert column (0-based index)")

class AddRowInput(BaseModel):
    """Input for add_row_to_sheet tool"""
    row_data: dict = Field(description="Dictionary of column names and values for the new row")
    position: Optional[int] = Field(default=None, description="Position to insert row (0-based index)")

@tool(args_schema=FilterDataInput)
def filter_sheets_data(condition: str, target_sheet: Optional[str] = None) -> str:
    """
    Filter Google Sheets data based on conditions and save to a new sheet.
    
    Use pandas query syntax for conditions:
    - Numeric: 'salary > 50000', 'age >= 25'
    - String: 'name == "John"', 'department.str.contains("Sales")'
    - Multiple conditions: 'salary > 50000 and age < 40'
    
    Returns confirmation message with number of filtered rows.
    """
    try:
        current_sheet = getattr(filter_sheets_data, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        if df.empty:
            return f"Source sheet '{current_sheet}' is empty or has no data to filter."
        
        filtered_df = df.query(condition)
        
        if target_sheet is None:
            target_sheet = f"{current_sheet}_filtered"
        
        result = sheets_service.write_to_sheet(filtered_df, target_sheet)
        
        if filtered_df.empty:
            return f"Filter condition '{condition}' returned no results. {result}"
        else:
            return f"Filtered {len(filtered_df)} rows to '{target_sheet}'. {result}"
            
    except Exception as e:
        return f"Filter error: {e}. Available columns: {list(df.columns) if 'df' in locals() else 'Unknown'}"

@tool(args_schema=AggregateDataInput)
def aggregate_sheets_data(group_by: str, agg_column: str, agg_method: str, target_sheet: Optional[str] = None) -> str:
    """
    Aggregate Google Sheets data by grouping and summarizing columns.
    
    Available aggregation methods: sum, mean, count, min, max
    
    Example: Group by 'department', sum 'salary' column
    
    Returns confirmation message with aggregated results location.
    """
    try:
        current_sheet = getattr(aggregate_sheets_data, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        if df.empty:
            return f"Source sheet '{current_sheet}' is empty or has no data to aggregate."
        
        # Check if required columns exist
        available_cols = list(df.columns)
        if group_by not in available_cols:
            return f"Aggregation error: Column '{group_by}' not found. Available columns: {available_cols}"
        
        if agg_method.lower() != 'count' and agg_column not in available_cols:
            return f"Aggregation error: Column '{agg_column}' not found. Available columns: {available_cols}"
        
        # Perform aggregation
        if agg_method.lower() == 'count':
            result = df.groupby(group_by).size().reset_index(name='count')
        else:
            result = df.groupby(group_by)[agg_column].agg(agg_method).reset_index()
        
        # Clean the result DataFrame
        result = result.fillna('')  # Replace NaN with empty strings
        
        if target_sheet is None:
            target_sheet = f"{current_sheet}_aggregated"
        
        write_result = sheets_service.write_to_sheet(result, target_sheet)
        return f"Aggregated data by '{group_by}' using {agg_method} method. Created {len(result)} rows. {write_result}"
    except Exception as e:
        return f"Aggregation error: {e}. Available columns: {list(df.columns) if 'df' in locals() else 'Unknown'}"

@tool(args_schema=PivotTableInput)
def create_pivot_table(index_col: str, columns_col: str, values_col: str, agg_func: str = "sum", target_sheet: Optional[str] = None) -> str:
    """
    Create a pivot table from Google Sheets data.
    
    - index_col: Column for pivot table rows
    - columns_col: Column for pivot table columns 
    - values_col: Column for pivot table values
    - agg_func: How to aggregate values (sum, mean, count, min, max)
    
    Returns confirmation message with pivot table location.
    """
    try:
        current_sheet = getattr(create_pivot_table, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        if df.empty:
            return f"Source sheet '{current_sheet}' is empty or has no data for pivot table."
        
        # Check if required columns exist
        available_cols = list(df.columns)
        missing_cols = []
        
        if index_col not in available_cols:
            missing_cols.append(f"index_col '{index_col}'")
        if columns_col not in available_cols:
            missing_cols.append(f"columns_col '{columns_col}'")
        if values_col not in available_cols:
            missing_cols.append(f"values_col '{values_col}'")
        
        if missing_cols:
            return f"Pivot table error: Missing columns: {', '.join(missing_cols)}. Available columns: {available_cols}"
        
        pivot_df = pd.pivot_table(df, index=index_col, columns=columns_col, 
                                values=values_col, aggfunc=agg_func, fill_value=0)
        
        # Reset index to make it writable
        pivot_df = pivot_df.reset_index()
        
        # Clean the result DataFrame
        pivot_df = pivot_df.fillna('')  # Replace NaN with empty strings
        
        if target_sheet is None:
            target_sheet = f"{current_sheet}_pivot"
        
        write_result = sheets_service.write_to_sheet(pivot_df, target_sheet)
        return f"Created pivot table with {len(pivot_df)} rows in '{target_sheet}'. {write_result}"
        
    except Exception as e:
        return f"Pivot table error: {e}. Available columns: {list(df.columns) if 'df' in locals() else 'Unknown'}"

@tool(args_schema=SortDataInput)
def sort_sheets_data(sort_column: str, ascending: bool = True, target_sheet: Optional[str] = None) -> str:
    """
    Sort Google Sheets data by a specified column.
    
    - sort_column: Name of column to sort by
    - ascending: True for ascending order, False for descending
    - target_sheet: Optional new sheet name, or sorts in place if None
    
    Returns confirmation message with sorted data location.
    """
    try:
        current_sheet = getattr(sort_sheets_data, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        sorted_df = df.sort_values(by=sort_column, ascending=ascending)
        
        if target_sheet:
            write_result = sheets_service.write_to_sheet(sorted_df, target_sheet)
        else:
            write_result = sheets_service.write_to_sheet(sorted_df, current_sheet)
            
        return f"Sorted data by {sort_column} ({'ascending' if ascending else 'descending'}). {write_result}"
    except Exception as e:
        return f"Sort error: {e}"

@tool(args_schema=AddColumnInput)
def add_column_to_sheet(column_name: str, formula: Optional[str] = None, default_value: str = "", position: Optional[int] = None) -> str:
    """
    Add a new column to the Google Sheet.
    
    - column_name: Name of the new column
    - formula: Pandas expression to calculate values (e.g., 'salary * 1.2', 'name.str.upper()')
    - default_value: Default value if no formula provided
    - position: Where to insert column (0-based index)
    
    Returns confirmation message with column addition details.
    """
    try:
        current_sheet = getattr(add_column_to_sheet, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        if formula:
            df[column_name] = df.eval(formula)
        else:
            df[column_name] = default_value
        
        # If position specified, reorder columns
        if position is not None and position < len(df.columns):
            cols = df.columns.tolist()
            cols.insert(position, cols.pop())  # Move new column to position
            df = df[cols]
        
        write_result = sheets_service.write_to_sheet(df, current_sheet)
        return f"Added column '{column_name}' to sheet. {write_result}"
    except Exception as e:
        return f"Add column error: {e}"

@tool(args_schema=AddRowInput)
def add_row_to_sheet(row_data: dict, position: Optional[int] = None) -> str:
    """
    Add a new row to the Google Sheet.
    
    - row_data: Dictionary mapping column names to values
    - position: Where to insert row (0-based index), appends at end if None
    
    Example row_data: {"name": "John Doe", "age": 30, "salary": 75000}
    
    Returns confirmation message with row addition details.
    """
    try:
        current_sheet = getattr(add_row_to_sheet, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        # Create new row DataFrame
        new_row = pd.DataFrame([row_data])
        
        if position is not None and position < len(df):
            # Insert at specific position
            df = pd.concat([df.iloc[:position], new_row, df.iloc[position:]], ignore_index=True)
        else:
            # Append at end
            df = pd.concat([df, new_row], ignore_index=True)
        
        write_result = sheets_service.write_to_sheet(df, current_sheet)
        return f"Added new row to sheet. {write_result}"
    except Exception as e:
        return f"Add row error: {e}"

@tool
def get_sheet_info() -> str:
    """
    Get information about the current Google Sheet including column names and sample data.
    
    Returns details about available columns and data structure.
    """
    try:
        current_sheet = getattr(get_sheet_info, '_current_sheet', 'Sheet1')
        df = sheets_service.read_sheet(current_sheet)
        
        if df.empty:
            return f"Sheet '{current_sheet}' is empty or has no data."
        
        columns = list(df.columns)
        sample_data = df.head(3).to_dict('records') if len(df) > 0 else []
        row_count = len(df)
        
        return f"""Sheet: {current_sheet}
Columns: {columns}
Total rows: {row_count}
Sample data: {sample_data}"""
        
    except Exception as e:
        return f"Error getting sheet info: {str(e)}"

@tool
def write_custom_results(data: str, sheet_name: str, start_row: int = 1, start_col: str = 'A') -> str:
    """
    Write custom results to a specific location in Google Sheets.
    
    - data: Data to write (will be processed appropriately)
    - sheet_name: Target sheet name
    - start_row: Starting row number (1-based)
    - start_col: Starting column letter (A, B, C, etc.)
    
    Returns confirmation message with write location.
    """
    try:
        # For now, write simple text data
        if isinstance(data, dict):
            # Convert dict to DataFrame for writing
            df = pd.DataFrame(list(data.items()), columns=['Key', 'Value'])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([["Result", data]], columns=['Key', 'Value'])
        
        start_cell = f"{start_col}{start_row}"
        write_result = sheets_service.write_to_sheet(df, sheet_name, start_cell)
        return f"Wrote custom results to {sheet_name}. {write_result}"
    except Exception as e:
        return f"Write custom results error: {e}"

# List of all tools for easy import
SHEETS_TOOLS = [
    filter_sheets_data,
    aggregate_sheets_data,
    create_pivot_table,
    sort_sheets_data,
    add_column_to_sheet,
    add_row_to_sheet,
    get_sheet_info,
    write_custom_results
]

def set_current_sheet_for_tools(sheet_name: str):
    """Set the current sheet name for all tools"""
    for tool_func in SHEETS_TOOLS:
        setattr(tool_func, '_current_sheet', sheet_name)
        
        

import os
import pandas as pd
from datetime import datetime

def get_file_info(filepath):
    """Get information about a file"""
    try:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        file_info = {
            'filename': os.path.basename(filepath),
            'filepath': filepath,
            'filesize': os.path.getsize(filepath) / 1024,  # Size in KB
            'created_time': datetime.fromtimestamp(os.path.getctime(filepath)).strftime('%Y-%m-%d %H:%M:%S'),
            'modified_time': datetime.fromtimestamp(os.path.getmtime(filepath)).strftime('%Y-%m-%d %H:%M:%S'),
            'is_excel': filepath.lower().endswith(('.xlsx', '.xls'))
        }
        
        # If it's an Excel file, get additional info
        if file_info['is_excel']:
            try:
                excel_file = pd.ExcelFile(filepath)
                file_info['sheet_count'] = len(excel_file.sheet_names)
                file_info['sheet_names'] = excel_file.sheet_names
                
                # Get row and column count from first sheet
                df = pd.read_excel(filepath, sheet_name=0)
                file_info['row_count'] = len(df)
                file_info['column_count'] = len(df.columns)
            except Exception as e:
                file_info['excel_read_error'] = str(e)
        
        return file_info
    except Exception as e:
        return {'error': str(e)}

def log_activity(activity, db, username=None):
    """Log an activity in the database"""
    if db:
        db.log_activity(activity, username)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {activity}")

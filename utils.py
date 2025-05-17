import os
import pandas as pd
from datetime import datetime

def get_file_info(file_path):
    """Get information about a file"""
    try:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / 1024  # Size in KB
            file_name = os.path.basename(file_path)
            return {
                "filename": file_name,
                "filepath": file_path,
                "filesize": file_size
            }
        return None
    except Exception as e:
        print(f"Error getting file info: {e}")
        return None
def log_activity(activity, db, username=None):
    """Log an activity in the database
    
    This is a wrapper function to make it easier to log activities
    from various parts of the application.
    """
    try:
        db.log_activity(activity, username)
        print(f"Activity logged: {activity} by {username}")
        return True
    except Exception as e:
        print(f"Error logging activity: {e}")
        return False
    
   
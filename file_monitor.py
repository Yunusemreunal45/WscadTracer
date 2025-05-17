import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from datetime import datetime
import threading

class ExcelFileHandler(FileSystemEventHandler):
    """Handler for Excel file events"""
    
    def __init__(self, db, excel_processor=None):
        """Initialize the handler with a database connection"""
        self.db = db
        self.excel_processor = excel_processor
        self.processed_files = set()  # Track processed files to avoid duplicates
    
    def is_excel_file(self, path):
        """Check if a file is an Excel file"""
        return path.lower().endswith(('.xlsx', '.xls'))
    
    def on_created(self, event):
        """Handle file creation events"""
        if not event.is_directory and self.is_excel_file(event.src_path):
            # Wait briefly to ensure the file is fully written
            time.sleep(1)
            
            # Check if file exists and is readable
            if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
                # Check if file is already processed
                if event.src_path in self.processed_files:
                    return
                
                # Check if it's a WSCAD Excel file
                try:
                    if self.excel_processor and not self.excel_processor.is_wscad_excel(event.src_path):
                        print(f"Skipping non-WSCAD Excel file: {event.src_path}")
                        return
                except Exception as e:
                    print(f"Error checking WSCAD format: {e}")
                    # Continue anyway, as it might still be a valid Excel file
                
                # Get file details
                filename = os.path.basename(event.src_path)
                filesize = os.path.getsize(event.src_path) / 1024  # Size in KB
                
                # Add file to database
                file_id = self.db.add_file(filename, event.src_path, filesize)
                
                if file_id:
                    self.db.log_activity(f"New Excel file detected: {filename}")
                    print(f"Added Excel file to database: {filename}")
                    
                    # Mark file as processed to avoid duplicates
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Error adding file to database: {filename}")
    
    def on_modified(self, event):
        """Handle file modification events"""
        if not event.is_directory and self.is_excel_file(event.src_path):
            # Wait briefly to ensure the file is fully written
            time.sleep(1)
            
            # Check if file exists and is readable
            if os.path.exists(event.src_path) and os.access(event.src_path, os.R_OK):
                # Skip if recently processed to avoid duplicates
                if event.src_path in self.processed_files:
                    # Remove from set to allow future modifications
                    self.processed_files.remove(event.src_path)
                    return
                
                # Check if it's a WSCAD Excel file
                try:
                    if self.excel_processor and not self.excel_processor.is_wscad_excel(event.src_path):
                        return
                except Exception:
                    # Continue anyway, as it might still be a valid Excel file
                    pass
                
                # Get file details
                filename = os.path.basename(event.src_path)
                filesize = os.path.getsize(event.src_path) / 1024  # Size in KB
                
                # Update file in database (this will create a new revision)
                file_id = self.db.add_file(filename, event.src_path, filesize)
                
                if file_id:
                    self.db.log_activity(f"Excel file modified: {filename}")
                    print(f"Updated Excel file in database: {filename}")
                    
                    # Mark file as processed to avoid duplicates
                    self.processed_files.add(event.src_path)
                else:
                    print(f"Error updating file in database: {filename}")

class FileMonitor:
    """Class for monitoring directories for Excel files"""
    
    def __init__(self, directory, db, excel_processor=None):
        """Initialize the file monitor with a directory and database connection"""
        self.directory = directory
        self.db = db
        self.observer = None
        self.excel_processor = excel_processor
        self.stop_event = threading.Event()
    
    def start_monitoring(self):
        """Start monitoring the directory for Excel files"""
        if not os.path.exists(self.directory):
            raise FileNotFoundError(f"Directory not found: {self.directory}")
        
        # Create event handler and observer
        event_handler = ExcelFileHandler(self.db, self.excel_processor)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.directory, recursive=True)
        
        # Start the observer
        self.observer.start()
        print(f"Started monitoring directory: {self.directory}")
        
        try:
            # Check for existing Excel files
            self.scan_existing_files(event_handler)
            
            # Keep running until stopped
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_monitoring()
        except Exception as e:
            print(f"Error during monitoring: {e}")
            self.stop_monitoring()
    
    def scan_existing_files(self, event_handler):
        """Scan for existing Excel files in the directory"""
        for root, _, files in os.walk(self.directory):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    file_path = os.path.join(root, file)
                    # Create a mock event to process the file
                    mock_event = type('Event', (), {'is_directory': False, 'src_path': file_path})
                    event_handler.on_created(mock_event)
    
    def stop_monitoring(self):
        """Stop monitoring the directory"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            print("Stopped directory monitoring")
        
        self.stop_event.set()

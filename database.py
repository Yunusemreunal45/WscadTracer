import os
import sqlite3
from datetime import datetime

class Database:
    """Database class for managing SQLite database operations"""
    
    def __init__(self, db_file="wscad_comparison.db"):
        """Initialize database connection"""
        self.db_file = db_file
        self.connection = None
        # Bağlantıyı hemen kurma, ihtiyaç olduğunda kur (thread güvenliği için)
    
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.connection = sqlite3.connect(self.db_file)
            self.connection.row_factory = sqlite3.Row
            print(f"Connected to SQLite database: {self.db_file}")
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def setup_database(self):
        """Create database tables if they don't exist"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            # Users table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
            ''')
            
            # Files table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                filepath TEXT NOT NULL,
                filesize INTEGER,
                detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT 0,
                current_revision INTEGER DEFAULT 1
            )
            ''')
            
            # File revisions table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                revision_number INTEGER NOT NULL,
                revision_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                revision_path TEXT NOT NULL,
                FOREIGN KEY (file_id) REFERENCES files(id)
            )
            ''')
            
            # Comparison results table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                revision1_id INTEGER NOT NULL,
                revision2_id INTEGER NOT NULL,
                changes_count INTEGER,
                comparison_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (file_id) REFERENCES files(id),
                FOREIGN KEY (revision1_id) REFERENCES file_revisions(id),
                FOREIGN KEY (revision2_id) REFERENCES file_revisions(id)
            )
            ''')
            
            # Activity logs table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                activity TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.connection.commit()
            print("Database tables created successfully")
            return True
        except Exception as e:
            print(f"Database setup error: {e}")
            return False
    
    def execute(self, query, params=None):
        """Execute SQL query with parameters"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            self.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"SQL execution error: {e}")
            return None
    
    def query(self, query, params=None):
        """Execute query and return all results"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries if using Row factory
            return rows
        except Exception as e:
            print(f"SQL query error: {e}")
            return []
    
    def query_one(self, query, params=None):
        """Execute query and return one result"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            row = cursor.fetchone()
            
            return row
        except Exception as e:
            print(f"SQL query error: {e}")
            return None
    
    # File management functions
    def add_file(self, filename, filepath, filesize):
        """Add a new file to the database"""
        try:
            # Check if file already exists
            existing_file = self.query_one("SELECT * FROM files WHERE filepath = ?", (filepath,))
            if existing_file:
                # Increment revision number
                new_revision = existing_file['current_revision'] + 1
                self.execute("UPDATE files SET current_revision = ? WHERE id = ?", 
                            (new_revision, existing_file['id']))
                
                # Add new revision
                self.execute(
                    "INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (?, ?, ?)",
                    (existing_file['id'], new_revision, filepath)
                )
                
                return existing_file['id']
            else:
                # Add new file
                file_id = self.execute(
                    "INSERT INTO files (filename, filepath, filesize, detected_time) VALUES (?, ?, ?, ?)",
                    (filename, filepath, filesize, datetime.now())
                )
                
                # Add first revision
                self.execute(
                    "INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (?, ?, ?)",
                    (file_id, 1, filepath)
                )
                
                return file_id
        except Exception as e:
            print(f"Error adding file: {e}")
            return None
    
    def get_all_files(self):
        """Get all files from the database"""
        return self.query("SELECT id, filename, filepath, filesize, detected_time, processed, current_revision FROM files ORDER BY detected_time DESC")
    
    def get_file_by_id(self, file_id):
        """Get file by ID"""
        result = self.query_one("SELECT * FROM files WHERE id = ?", (file_id,))
        if result:
            return dict(result)
        return None
    
    def mark_file_as_processed(self, file_id):
        """Mark a file as processed"""
        return self.execute("UPDATE files SET processed = 1 WHERE id = ?", (file_id,))
    
    def get_file_revisions(self, file_id):
        """Get all revisions for a file"""
        return self.query("""
            SELECT fr.id, fr.file_id, fr.revision_number, fr.revision_date, fr.revision_path
            FROM file_revisions fr
            WHERE fr.file_id = ?
            ORDER BY fr.revision_number DESC
        """, (file_id,))
    
    def get_revision_by_id(self, revision_id):
        """Get revision by ID"""
        result = self.query_one("SELECT * FROM file_revisions WHERE id = ?", (revision_id,))
        if result:
            return dict(result)
        return None
    
    def save_comparison_result(self, file_id, rev1_id, rev2_id, changes_count, comparison_date):
        """Save comparison result to database"""
        return self.execute("""
            INSERT INTO comparisons (file_id, revision1_id, revision2_id, changes_count, comparison_date)
            VALUES (?, ?, ?, ?, ?)
        """, (file_id, rev1_id, rev2_id, changes_count, comparison_date))
    
    def get_comparison_history(self, file_id=None):
        """Get comparison history from database"""
        if file_id:
            return self.query("""
                SELECT c.id, c.file_id, f.filename, c.revision1_id, c.revision2_id, c.changes_count, c.comparison_date
                FROM comparisons c
                JOIN files f ON c.file_id = f.id
                WHERE c.file_id = ?
                ORDER BY c.comparison_date DESC
            """, (file_id,))
        else:
            return self.query("""
                SELECT c.id, c.file_id, f.filename, c.revision1_id, c.revision2_id, c.changes_count, c.comparison_date
                FROM comparisons c
                JOIN files f ON c.file_id = f.id
                ORDER BY c.comparison_date DESC
            """)
    
    # Activity logging
    def log_activity(self, activity, username=None):
        """Log an activity in the database"""
        return self.execute(
            "INSERT INTO activity_logs (username, activity, timestamp) VALUES (?, ?, ?)",
            (username, activity, datetime.now())
        )
    
    def get_activity_logs(self, limit=100):
        """Get activity logs from database"""
        return self.query("""
            SELECT id, username, activity, timestamp 
            FROM activity_logs 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,))
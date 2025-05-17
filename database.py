import os
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

class Database:
    """Database class for managing PostgreSQL database operations via SQLAlchemy"""
    
    def __init__(self, connection_string=None):
        """Initialize database connection"""
        # Load environment variables from .env file
        load_dotenv()
        
        # Use provided connection string or get from environment
        self.connection_string = connection_string or os.getenv("DATABASE_URL")
        self.engine = None
        self.connection = None
        self.connect()
    
    def connect(self):
        """Connect to the PostgreSQL database"""
        try:
            if not self.connection_string:
                raise ValueError("Database connection string not provided. Please set DATABASE_URL environment variable.")
            
            # Create SQLAlchemy engine
            self.engine = create_engine(self.connection_string)
            self.connection = self.engine.connect()
            print("Connected to PostgreSQL database successfully")
        except Exception as e:
            print(f"Database connection error: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
    
    def setup_database(self):
        """Create database tables if they don't exist"""
        try:
            if not self.connection:
                self.connect()
                if not self.connection:
                    return False
            
            # Users table
            self.connection.execute(text('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
            '''))
            
            # Files table
            self.connection.execute(text('''
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                filepath TEXT NOT NULL,
                filesize INTEGER,
                detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                current_revision INTEGER DEFAULT 1
            )
            '''))
            
            # File revisions table
            self.connection.execute(text('''
            CREATE TABLE IF NOT EXISTS file_revisions (
                id SERIAL PRIMARY KEY,
                file_id INTEGER NOT NULL,
                revision_number INTEGER NOT NULL,
                revision_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                revision_path TEXT NOT NULL,
                FOREIGN KEY (file_id) REFERENCES files(id)
            )
            '''))
            
            # Comparison results table
            self.connection.execute(text('''
            CREATE TABLE IF NOT EXISTS comparisons (
                id SERIAL PRIMARY KEY,
                file_id INTEGER NOT NULL,
                revision1_id INTEGER NOT NULL,
                revision2_id INTEGER NOT NULL,
                changes_count INTEGER,
                comparison_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (file_id) REFERENCES files(id),
                FOREIGN KEY (revision1_id) REFERENCES file_revisions(id),
                FOREIGN KEY (revision2_id) REFERENCES file_revisions(id)
            )
            '''))
            
            # Activity logs table
            self.connection.execute(text('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                id SERIAL PRIMARY KEY,
                username TEXT,
                activity TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''))
            
            return True
        except SQLAlchemyError as e:
            print(f"Database setup error: {e}")
            return False
    
    def execute(self, query, params=None):
        """Execute SQL query with parameters"""
        try:
            if not self.connection:
                self.connect()
                if not self.connection:
                    return None
                    
            # Convert query to SQLAlchemy text object and handle parameters
            sql_query = text(query)
            
            # Execute and return the result
            result = self.connection.execute(sql_query, params if params else {})
            
            # For INSERT statements, you typically want to return the new ID
            # PostgreSQL's way to get the last insert ID
            if query.strip().upper().startswith("INSERT"):
                # Try to get the last inserted ID
                last_id_query = text("SELECT lastval()")
                try:
                    last_id_result = self.connection.execute(last_id_query)
                    return last_id_result.scalar()
                except Exception:
                    # If not possible, just return success
                    return True
            
            return True
        except SQLAlchemyError as e:
            print(f"SQL execution error: {e}")
            return None
    
    def query(self, query, params=None):
        """Execute query and return all results"""
        try:
            if not self.connection:
                self.connect()
                if not self.connection:
                    return []
                    
            # Convert query to SQLAlchemy text object
            sql_query = text(query)
            
            # Execute query
            result = self.connection.execute(sql_query, params if params else {})
            
            # Fetch all results
            rows = result.fetchall()
            
            # Convert to list of dictionaries
            return [dict(row) for row in rows]
        except SQLAlchemyError as e:
            print(f"SQL query error: {e}")
            return []
    
    def query_one(self, query, params=None):
        """Execute query and return one result"""
        try:
            if not self.connection:
                self.connect()
                if not self.connection:
                    return None
                    
            # Convert query to SQLAlchemy text object
            sql_query = text(query)
            
            # Execute query
            result = self.connection.execute(sql_query, params if params else {})
            
            # Fetch one result
            row = result.fetchone()
            
            # Convert to dictionary
            return dict(row) if row else None
        except SQLAlchemyError as e:
            print(f"SQL query error: {e}")
            return None
    
    # File management functions
    def add_file(self, filename, filepath, filesize):
        """Add a new file to the database"""
        try:
            # Check if file already exists
            existing_file = self.query_one("SELECT * FROM files WHERE filepath = :filepath", {"filepath": filepath})
            if existing_file:
                # Increment revision number
                new_revision = existing_file['current_revision'] + 1
                self.execute("UPDATE files SET current_revision = :revision WHERE id = :id", 
                            {"revision": new_revision, "id": existing_file['id']})
                
                # Add new revision
                self.execute(
                    "INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (:file_id, :revision, :path)",
                    {"file_id": existing_file['id'], "revision": new_revision, "path": filepath}
                )
                
                return existing_file['id']
            else:
                # Add new file
                file_id = self.execute(
                    "INSERT INTO files (filename, filepath, filesize, detected_time) VALUES (:filename, :filepath, :filesize, :detected_time)",
                    {"filename": filename, "filepath": filepath, "filesize": filesize, "detected_time": datetime.now()}
                )
                
                # Add first revision
                self.execute(
                    "INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (:file_id, :revision, :path)",
                    {"file_id": file_id, "revision": 1, "path": filepath}
                )
                
                return file_id
        except SQLAlchemyError as e:
            print(f"Error adding file: {e}")
            return None
    
    def get_all_files(self):
        """Get all files from the database"""
        return self.query("SELECT id, filename, filepath, filesize, detected_time, processed, current_revision FROM files ORDER BY detected_time DESC")
    
    def get_file_by_id(self, file_id):
        """Get file by ID"""
        result = self.query_one("SELECT * FROM files WHERE id = :id", {"id": file_id})
        return result
    
    def mark_file_as_processed(self, file_id):
        """Mark a file as processed"""
        return self.execute("UPDATE files SET processed = TRUE WHERE id = :id", {"id": file_id})
    
    def get_file_revisions(self, file_id):
        """Get all revisions for a file"""
        return self.query("""
            SELECT fr.id, fr.file_id, fr.revision_number, fr.revision_date, fr.revision_path
            FROM file_revisions fr
            WHERE fr.file_id = :file_id
            ORDER BY fr.revision_number DESC
        """, {"file_id": file_id})
    
    def get_revision_by_id(self, revision_id):
        """Get revision by ID"""
        result = self.query_one("SELECT * FROM file_revisions WHERE id = :id", {"id": revision_id})
        return result
    
    def save_comparison_result(self, file_id, rev1_id, rev2_id, changes_count, comparison_date):
        """Save comparison result to database"""
        return self.execute("""
            INSERT INTO comparisons (file_id, revision1_id, revision2_id, changes_count, comparison_date)
            VALUES (:file_id, :rev1_id, :rev2_id, :changes_count, :comparison_date)
        """, {
            "file_id": file_id, 
            "rev1_id": rev1_id, 
            "rev2_id": rev2_id, 
            "changes_count": changes_count, 
            "comparison_date": comparison_date
        })
    
    def get_comparison_history(self, file_id=None):
        """Get comparison history from database"""
        if file_id:
            return self.query("""
                SELECT c.id, c.file_id, f.filename, c.revision1_id, c.revision2_id, c.changes_count, c.comparison_date
                FROM comparisons c
                JOIN files f ON c.file_id = f.id
                WHERE c.file_id = :file_id
                ORDER BY c.comparison_date DESC
            """, {"file_id": file_id})
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
            "INSERT INTO activity_logs (username, activity, timestamp) VALUES (:username, :activity, :timestamp)",
            {"username": username, "activity": activity, "timestamp": datetime.now()}
        )
    
    def get_activity_logs(self, limit=100):
        """Get activity logs from database"""
        return self.query("""
            SELECT id, username, activity, timestamp 
            FROM activity_logs 
            ORDER BY timestamp DESC 
            LIMIT :limit
        """, {"limit": limit})

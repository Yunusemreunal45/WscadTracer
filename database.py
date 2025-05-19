import os
import sqlite3
import threading
from datetime import datetime

class Database:
    """Thread-safe SQLite database handler"""

    def __init__(self, db_file="wscad_comparison.db"):
        self.db_file = db_file
        self._connection = None
        self._lock = threading.Lock()

    def setup_database(self):
        try:
            with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    filepath TEXT NOT NULL,
                    filesize INTEGER,
                    detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT 0,
                    current_revision INTEGER DEFAULT 1
                )''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    revision_number INTEGER NOT NULL,
                    revision_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    revision_path TEXT NOT NULL,
                    FOREIGN KEY (file_id) REFERENCES files(id)
                )''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS comparisons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    revision1_id INTEGER NOT NULL,
                    revision2_id INTEGER NOT NULL,
                    changes_count INTEGER,
                    comparison_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    comparison_data TEXT,
                    FOREIGN KEY (file_id) REFERENCES files(id),
                    FOREIGN KEY (revision1_id) REFERENCES file_revisions(id),
                    FOREIGN KEY (revision2_id) REFERENCES file_revisions(id)
                )''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    activity TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')

                conn.commit()
                print("Database tables created successfully")
                return True
        except Exception as e:
            print(f"Database setup error: {e}")
            return False

    def execute(self, query, params=None):
        try:
            with self._lock:  # Use thread lock for thread safety
                with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    try:
                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)
                        conn.commit()
                        return cursor.lastrowid
                    except sqlite3.Error as e:
                        conn.rollback()
                        raise e
        except Exception as e:
            print(f"SQL execution error: {e}")
            return None

    def query(self, query, params=None):
        try:
            with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"SQL query error: {e}")
            return []

    def query_one(self, query, params=None):
        try:
            with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchone()
        except Exception as e:
            print(f"SQL query error: {e}")
            return None

    def add_file(self, filename, filepath, filesize):
        try:
            # Check if file exists in database
            existing_file = self.query_one("SELECT * FROM files WHERE filepath = ?", (filepath,))
            
            # Clean up old files if more than 10 exist
            total_files = self.query_one("SELECT COUNT(*) as count FROM files")
            if total_files and total_files['count'] > 10:
                # Get oldest files
                old_files = self.query("""
                    SELECT id, filepath FROM files 
                    ORDER BY detected_time ASC 
                    LIMIT ?
                """, (total_files['count'] - 10,))
                
                # Delete old files
                for old_file in old_files:
                    if os.path.exists(old_file['filepath']):
                        os.remove(old_file['filepath'])
                    self.execute("DELETE FROM files WHERE id = ?", (old_file['id'],))
            
            if existing_file:
                new_revision = existing_file['current_revision'] + 1
                self.execute("UPDATE files SET current_revision = ? WHERE id = ?", (new_revision, existing_file['id']))
                self.execute("INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (?, ?, ?)",
                             (existing_file['id'], new_revision, filepath))
                return existing_file['id']
            else:
                file_id = self.execute("INSERT INTO files (filename, filepath, filesize, detected_time) VALUES (?, ?, ?, ?)",
                                      (filename, filepath, filesize, datetime.now()))
                self.execute("INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (?, ?, ?)",
                             (file_id, 1, filepath))
                return file_id
        except Exception as e:
            print(f"Error adding file: {e}")
            return None

    def get_all_files(self):
        return self.query("SELECT id, filename, filepath, filesize, detected_time, processed, current_revision FROM files ORDER BY detected_time DESC")

    def get_file_by_id(self, file_id):
        result = self.query_one("SELECT * FROM files WHERE id = ?", (file_id,))
        return dict(result) if result else None

    def mark_file_as_processed(self, file_id):
        return self.execute("UPDATE files SET processed = 1 WHERE id = ?", (file_id,))

    def get_file_revisions(self, file_id):
        return self.query("""
            SELECT fr.id, fr.file_id, fr.revision_number, fr.revision_date, fr.revision_path
            FROM file_revisions fr
            WHERE fr.file_id = ?
            ORDER BY fr.revision_number DESC
        """, (file_id,))

    def get_revision_by_id(self, revision_id):
        result = self.query_one("SELECT * FROM file_revisions WHERE id = ?", (revision_id,))
        return dict(result) if result else None

    def save_comparison_result(self, file_id, rev1_id, rev2_id, changes_count, comparison_date, comparison_data=None):
        try:
            with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Validate inputs
                if not all([file_id, rev1_id, rev2_id, changes_count, comparison_date]):
                    raise ValueError("Tüm alanlar zorunludur")

                # Save comparison details with JSON data
                cursor.execute("""
                    INSERT INTO comparisons (
                        file_id, 
                        revision1_id, 
                        revision2_id, 
                        changes_count, 
                        comparison_date,
                        comparison_data
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    file_id, 
                    rev1_id, 
                    rev2_id, 
                    changes_count, 
                    comparison_date,
                    json.dumps(comparison_data) if comparison_data else None
                ))
                
                # Check if file exists
                cursor.execute("SELECT id FROM files WHERE id = ?", (file_id,))
                if not cursor.fetchone():
                    raise ValueError(f"Dosya ID bulunamadı: {file_id}")
                
                # Insert comparison result
                cursor.execute("""
                    INSERT INTO comparisons (file_id, revision1_id, revision2_id, changes_count, comparison_date)
                    VALUES (?, ?, ?, ?, ?)
                """, (file_id, rev1_id, rev2_id, changes_count, comparison_date))
                
                comparison_id = cursor.lastrowid
                conn.commit()
                return comparison_id
        except Exception as e:
            print(f"Karşılaştırma sonucu kaydedilirken hata: {e}")
            return None

    def get_comparison_history(self, file_id=None):
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

    def log_activity(self, activity, username=None):
        return self.execute("INSERT INTO activity_logs (username, activity, timestamp) VALUES (?, ?, ?)",
                            (username, activity, datetime.now()))

    def get_activity_logs(self, limit=100):
        return self.query("""
            SELECT id, username, activity, timestamp
            FROM activity_logs
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,))
        
    def get_recent_files(self, limit=5):
        return self.query("""
            SELECT id, filename, filepath, filesize, detected_time
            FROM files
            ORDER BY detected_time DESC
            LIMIT ?
        """, (limit,))

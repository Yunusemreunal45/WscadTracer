import os
import sqlite3
import threading
from datetime import datetime
import json
import re

class Database:
    """Thread-safe SQLite database handler with project management"""

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

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )''')

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS project_comparisons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    comparison_id INTEGER NOT NULL,
                    display_name TEXT,
                    revision_number INTEGER,
                    FOREIGN KEY (project_id) REFERENCES projects(id),
                    FOREIGN KEY (comparison_id) REFERENCES comparisons(id)
                )''')      

                conn.commit()
                print("Database tables created successfully")
                return True
        except Exception as e:
            print(f"Database setup error: {e}")
            return False

    def execute(self, query, params=None):
        try:
            with self._lock:
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

    # Project management methods
    def create_project(self, name, description, created_by):
        """Create a new project"""
        try:
            project_id = self.execute(
                "INSERT INTO projects (name, description, created_by) VALUES (?, ?, ?)",
                (name, description, created_by)
            )
            return project_id
        except Exception as e:
            print(f"Error creating project: {e}")
            return None

    def get_all_projects(self):
        """Get all active projects"""
        return self.query("""
            SELECT id, name, description, created_by, created_at, is_active
            FROM projects
            WHERE is_active = 1
            ORDER BY created_at DESC
        """)

    def get_project_by_id(self, project_id):
        """Get project by ID"""
        result = self.query_one("SELECT * FROM projects WHERE id = ?", (project_id,))
        return dict(result) if result else None

    def update_project(self, project_id, name=None, description=None):
        """Update project details"""
        try:
            if name and description:
                return self.execute(
                    "UPDATE projects SET name = ?, description = ? WHERE id = ?",
                    (name, description, project_id)
                )
            elif name:
                return self.execute(
                    "UPDATE projects SET name = ? WHERE id = ?",
                    (name, project_id)
                )
            elif description:
                return self.execute(
                    "UPDATE projects SET description = ? WHERE id = ?",
                    (description, project_id)
                )
            return True
        except Exception as e:
            print(f"Error updating project: {e}")
            return False

    def delete_project(self, project_id):
        """Soft delete project (mark as inactive)"""
        try:
            return self.execute(
                "UPDATE projects SET is_active = 0 WHERE id = ?",
                (project_id,)
            )
        except Exception as e:
            print(f"Error deleting project: {e}")
            return False

    def clean_filename_for_display(self, filename):
        """Clean filename for display by removing extensions and revision suffixes"""
        # Remove file extension
        name_without_ext = os.path.splitext(filename)[0]
        
        # Remove common revision patterns
        patterns = [
            r'_rev\d+$',      # _rev1, _rev2, etc.
            r'-v\d+$',        # -v1, -v2, etc.
            r'_v\d+$',        # _v1, _v2, etc.
            r'\(\d+\)$',      # (1), (2), etc.
            r'_\d+$',         # _1, _2, etc. (only if at the end)
        ]
        
        cleaned_name = name_without_ext
        for pattern in patterns:
            cleaned_name = re.sub(pattern, '', cleaned_name, flags=re.IGNORECASE)
        
        return cleaned_name.strip()

    def add_comparison_to_project(self, project_id, comparison_id, file1_name, file2_name):
        """Add a comparison to a project with cleaned display name"""
        try:
            # Generate display name from filenames
            clean_name1 = self.clean_filename_for_display(file1_name)
            clean_name2 = self.clean_filename_for_display(file2_name)
            display_name = f"{clean_name1} vs {clean_name2}"
            
            # Get next revision number for this project
            last_revision = self.query_one("""
                SELECT MAX(revision_number) as max_rev 
                FROM project_comparisons 
                WHERE project_id = ?
            """, (project_id,))
            
            next_revision = (last_revision['max_rev'] or 0) + 1 if last_revision else 1
            
            project_comp_id = self.execute("""
                INSERT INTO project_comparisons 
                (project_id, comparison_id, display_name, revision_number)
                VALUES (?, ?, ?, ?)
            """, (project_id, comparison_id, display_name, next_revision))
            
            return project_comp_id
        except Exception as e:
            print(f"Error adding comparison to project: {e}")
            return None

    def get_project_revisions(self, project_id):
        """Get all revisions (comparisons) for a project"""
        return self.query("""
            SELECT 
                pc.id,
                pc.display_name,
                pc.revision_number,
                c.changes_count,
                c.comparison_date,
                c.comparison_data,
                c.id as comparison_id
            FROM project_comparisons pc
            JOIN comparisons c ON pc.comparison_id = c.id
            WHERE pc.project_id = ?
            ORDER BY pc.revision_number DESC
        """, (project_id,))

    def get_project_revision_details(self, project_id, revision_number):
        """Get detailed information about a specific project revision"""
        return self.query_one("""
            SELECT 
                pc.id,
                pc.display_name,
                pc.revision_number,
                c.changes_count,
                c.comparison_date,
                c.comparison_data,
                c.file_id,
                c.revision1_id,
                c.revision2_id,
                f.filename as base_filename,
                fr1.revision_path as file1_path,
                fr2.revision_path as file2_path
            FROM project_comparisons pc
            JOIN comparisons c ON pc.comparison_id = c.id
            JOIN files f ON c.file_id = f.id
            LEFT JOIN file_revisions fr1 ON c.revision1_id = fr1.id
            LEFT JOIN file_revisions fr2 ON c.revision2_id = fr2.id
            WHERE pc.project_id = ? AND pc.revision_number = ?
        """, (project_id, revision_number))

    # Existing methods (keeping all original functionality)
    def add_file(self, filename, filepath, filesize):
        try:
            existing_file = self.query_one("SELECT * FROM files WHERE filepath = ?", (filepath,))
            
            total_files = self.query_one("SELECT COUNT(*) as count FROM files")
            if total_files and total_files['count'] > 10:
                old_files = self.query("""
                    SELECT id, filepath FROM files 
                    ORDER BY detected_time ASC 
                    LIMIT ?
                """, (total_files['count'] - 10,))
                
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
                
                if not all([file_id, rev1_id, rev2_id, changes_count, comparison_date]):
                    raise ValueError("Tüm alanlar zorunludur")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comparisons (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id INTEGER,
                        revision1_id INTEGER,
                        revision2_id INTEGER,
                        changes_count INTEGER,
                        comparison_date DATETIME,
                        comparison_data TEXT,
                        FOREIGN KEY (file_id) REFERENCES files(id),
                        FOREIGN KEY (revision1_id) REFERENCES file_revisions(id),
                        FOREIGN KEY (revision2_id) REFERENCES file_revisions(id)
                    )
                """)
                
                cursor.execute("SELECT id FROM files WHERE id = ?", (file_id,))
                if not cursor.fetchone():
                    raise ValueError(f"Dosya ID bulunamadı: {file_id}")
                
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
                
                comparison_id = cursor.lastrowid
                conn.commit()
                return comparison_id
        except Exception as e:
            print(f"Karşılaştırma sonucu kaydedilirken hata: {e}")
            return None
    

    def get_comparison_history(self, file_id=None):
         """
       Yalnızca comparison kayıtlarını ve ilgili dosya adlarını getirir
        """
         return self.query("""
        SELECT 
            c.id, 
            c.comparison_date, 
            c.changes_count, 
            f.filename
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
    
    
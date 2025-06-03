import os
import sqlite3
import threading
from datetime import datetime
import json
import re

class Database:
    """Thread-safe SQLite database handler with WSCAD BOM project management"""

    def __init__(self, db_file="wscad_comparison.db"):
        """Initialize database connection"""
        self.db_file = db_file
        self.conn = None
        self._lock = threading.Lock()
        self.setup_database()

    def setup_database(self):
        """Initialize database tables with improved structure"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                
                # Create users table first
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        email TEXT UNIQUE,
                        full_name TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        last_login DATETIME,
                        is_active BOOLEAN DEFAULT 1
                    )
                """)

                # Create projects table with optimized columns
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS projects (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT,
                        created_by TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT 1,
                        supabase_id TEXT UNIQUE,
                        sync_status TEXT DEFAULT 'pending',
                        project_type TEXT DEFAULT 'wscad',
                        UNIQUE(name COLLATE NOCASE)
                    )
                """)

                # Check if columns exist and add them if they don't
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(projects)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'updated_at' not in columns:
                    conn.execute("ALTER TABLE projects ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                
                if 'project_type' not in columns:
                    conn.execute("ALTER TABLE projects ADD COLUMN project_type TEXT DEFAULT 'wscad'")
                
                if 'sync_status' not in columns:
                    conn.execute("ALTER TABLE projects ADD COLUMN sync_status TEXT DEFAULT 'pending'")
                
                if 'supabase_id' not in columns:
                    conn.execute("ALTER TABLE projects ADD COLUMN supabase_id TEXT")

                # Create activity logs table with optimized columns
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS activity_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        activity TEXT NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        project_id INTEGER,
                        activity_type TEXT DEFAULT 'general',
                        FOREIGN KEY (project_id) REFERENCES projects(id)
                    )
                """)

                # Check if activity_type column exists and add it if it doesn't
                cursor.execute("PRAGMA table_info(activity_logs)")
                activity_columns = [column[1] for column in cursor.fetchall()]
                
                if 'activity_type' not in activity_columns:
                    conn.execute("ALTER TABLE activity_logs ADD COLUMN activity_type TEXT DEFAULT 'general'")

                # Create WSCAD files table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_files (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        filename TEXT NOT NULL,
                        filepath TEXT,
                        filesize INTEGER,
                        detected_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT 0,
                        current_revision INTEGER DEFAULT 1,
                        is_emri_no TEXT,
                        proje_adi TEXT,
                        revizyon_no TEXT,
                        project_info TEXT,
                        file_hash TEXT,
                        UNIQUE(filepath COLLATE NOCASE)
                    )
                """)

                # Create WSCAD file revisions table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_file_revisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id INTEGER NOT NULL,
                        revision_number INTEGER NOT NULL,
                        revision_path TEXT NOT NULL,
                        is_emri_no TEXT,
                        revizyon_no TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        created_by TEXT,
                        file_hash TEXT,
                        UNIQUE(file_id, revision_number),
                        FOREIGN KEY (file_id) REFERENCES wscad_files(id)
                    )
                """)

                # Create WSCAD comparisons table - updated to match Supabase schema
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_comparisons (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        changes_count INTEGER DEFAULT 0,
                        comparison_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                        supabase_saved BOOLEAN DEFAULT 0,
                        supabase_comparison_id TEXT,
                        created_by TEXT
                    )
                """)

                # Create project comparisons table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS project_comparisons (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        project_id INTEGER NOT NULL,
                        comparison_id INTEGER,
                        display_name TEXT,
                        revision_number INTEGER,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        supabase_saved BOOLEAN DEFAULT 0,
                        supabase_revision_id TEXT,
                        FOREIGN KEY (project_id) REFERENCES projects(id),
                        FOREIGN KEY (comparison_id) REFERENCES wscad_comparisons(id)
                    )
                """)

                # Create indexes for better performance
                conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_logs_username ON activity_logs(username)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_logs_timestamp ON activity_logs(timestamp)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_logs_activity_type ON activity_logs(activity_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_projects_name ON projects(name)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_projects_project_type ON projects(project_type)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wscad_files_filename ON wscad_files(filename)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wscad_files_is_emri ON wscad_files(is_emri_no)")

                conn.commit()
                print("✅ Database tables created/updated successfully")
                return True
                
        except Exception as e:
            print(f"❌ Database setup error: {e}")
            return False

    def execute(self, query, params=None):
        """Execute SQL with improved error handling"""
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
                        print(f"SQL execution error: {e}")
                        print(f"Query: {query}")
                        print(f"Params: {params}")
                        raise e
        except Exception as e:
            print(f"Database execution error: {e}")
            return None

    def query(self, query, params=None):
        """Query database with improved error handling"""
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
            print(f"Query: {query}")
            print(f"Params: {params}")
            return []
    
    def query_one(self, query, params=None):
        """Query single record with improved error handling"""
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
            print(f"Query: {query}")
            print(f"Params: {params}")
            return None

    # Project Management Methods
    def create_project(self, name, description, created_by):
        """Create a new project with better error handling"""
        try:
            # Check if project already exists
            existing = self.query_one("SELECT id FROM projects WHERE name = ? AND is_active = 1", (name,))
            if existing:
                print(f"Project {name} already exists")
                return existing['id']
            
            project_id = self.execute("""
                INSERT INTO projects (name, description, created_by, created_at, updated_at, project_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, description, created_by, datetime.now(), datetime.now(), 'wscad'))
            
            if project_id:
                self.log_activity(f"Project created: {name}", created_by, project_id, activity_type='project')
                print(f"✅ Project created: {name} (ID: {project_id})")
            
            return project_id
            
        except Exception as e:
            print(f"❌ Project creation error: {e}")
            return None

    def get_all_projects(self):
        """Get all active projects with actual revision counts"""
        try:
            return self.query("""
                SELECT p.id, p.name, p.description, p.created_by, p.created_at, p.updated_at,
                       COALESCE((SELECT COUNT(*) FROM project_comparisons WHERE project_id = p.id), 0) AS current_revision,
                       p.is_active, p.supabase_id, p.sync_status, p.project_type
                FROM projects p
                WHERE p.is_active = 1
                ORDER BY p.updated_at DESC
            """)
        except Exception as e:
            print(f"Error getting projects: {e}")
            return []

    def get_project_by_id(self, project_id):
        """Get project by ID"""
        try:
            result = self.query_one("""
                SELECT id, name, description, created_by, created_at, updated_at, current_revision,
                       is_active, supabase_id, sync_status, project_type
                FROM projects 
                WHERE id = ? AND is_active = 1
            """, (project_id,))
            return dict(result) if result else None
        except Exception as e:
            print(f"Error getting project by id: {e}")
            return None

    def get_project_by_name(self, name):
        """Get project by name"""
        try:
            result = self.query_one("""
                SELECT id, name, description, created_by, created_at, updated_at, current_revision,
                       is_active, supabase_id, sync_status, project_type
                FROM projects 
                WHERE LOWER(name) = LOWER(?) AND is_active = 1
            """, (name,))
            return dict(result) if result else None
        except Exception as e:
            print(f"Error getting project by name: {e}")
            return None

    def update_project_revision(self, project_id):
        """Increment project revision number"""
        try:
            return self.execute("""
                UPDATE projects 
                SET current_revision = current_revision + 1, updated_at = ?
                WHERE id = ?
            """, (datetime.now(), project_id))
        except Exception as e:
            print(f"Error updating project revision: {e}")
            return False

    # WSCAD File Management
    def add_wscad_file(self, filename, filepath, filesize, project_info=None):
        """Add WSCAD BOM file with improved handling"""
        try:
            # Extract project information
            is_emri_no = ""
            proje_adi = ""
            revizyon_no = ""
            
            if project_info:
                is_emri_no = project_info.get('is_emri_no', '')
                proje_adi = project_info.get('proje_adi', '')
                revizyon_no = project_info.get('revizyon_no', '')

            # Check if file already exists
            existing_file = self.query_one("SELECT * FROM wscad_files WHERE filepath = ?", (filepath,))
            
            if existing_file:
                # Update existing file
                new_revision = existing_file['current_revision'] + 1
                file_id = self.execute("""
                    UPDATE wscad_files 
                    SET current_revision = ?, is_emri_no = ?, proje_adi = ?, 
                        revizyon_no = ?, project_info = ?, detected_time = ?
                    WHERE id = ?
                """, (new_revision, is_emri_no, proje_adi, revizyon_no, 
                      json.dumps(project_info) if project_info else None, 
                      datetime.now(), existing_file['id']))
                
                # Add revision record
                self.execute("""
                    INSERT INTO wscad_file_revisions 
                    (file_id, revision_number, revision_path, is_emri_no, revizyon_no) 
                    VALUES (?, ?, ?, ?, ?)
                """, (existing_file['id'], new_revision, filepath, is_emri_no, revizyon_no))
                
                return existing_file['id']
            else:
                # Add new file
                file_id = self.execute("""
                    INSERT INTO wscad_files 
                    (filename, filepath, filesize, is_emri_no, proje_adi, revizyon_no, project_info) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (filename, filepath, filesize, is_emri_no, proje_adi, revizyon_no, 
                      json.dumps(project_info) if project_info else None))
                
                if file_id:
                    # Add initial revision
                    self.execute("""
                        INSERT INTO wscad_file_revisions 
                        (file_id, revision_number, revision_path, is_emri_no, revizyon_no) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (file_id, 1, filepath, is_emri_no, revizyon_no))
                
                return file_id
                
        except Exception as e:
            print(f"Error adding WSCAD file: {e}")
            return None

    def get_all_wscad_files(self):
        """Get all WSCAD files"""
        return self.query("""
            SELECT id, filename, filepath, filesize, detected_time, processed, 
                   current_revision, is_emri_no, proje_adi, revizyon_no, project_info
            FROM wscad_files 
            ORDER BY detected_time DESC
        """)

    def get_recent_wscad_files(self, limit=10):
        """Get recently processed WSCAD files"""
        try:
            return self.query("""
                SELECT id, filename, filepath, filesize, detected_time, 
                       is_emri_no, proje_adi, revizyon_no
                FROM wscad_files
                ORDER BY detected_time DESC
                LIMIT ?
            """, (limit,))
        except Exception as e:
            print(f"Error getting recent files: {e}")
            return []

    # Comparison Management
    def save_comparison_result(self, file1_id=None, file2_id=None, project_id=None, 
                               changes_count=0, comparison_data=None, created_by=None):
        """Save comparison result with improved structure"""
        try:
            # Generate file names based on available information
            file1_name = f"File_{file1_id}" if file1_id else "Unknown"
            file2_name = f"File_{file2_id}" if file2_id else "Unknown"
            
            # Calculate changes_count if comparison_data is provided
            if comparison_data is not None and isinstance(comparison_data, list):
                changes_count = len(comparison_data)
            
            # First create the comparison record
            comparison_id = self.execute("""
                INSERT INTO wscad_comparisons 
                (changes_count, created_by)
                VALUES (?, ?)
            """, (changes_count, created_by))
            
            # Link to project if provided
            if comparison_id and project_id:
                self.add_comparison_to_project(project_id, comparison_id, 
                                              file1_name, file2_name)
            
            return comparison_id
            
        except Exception as e:
            print(f"SQL execution error: {e}\nQuery:\n                INSERT INTO wscad_comparisons\n\n                (changes_count, created_by)\n                VALUES (?, ?)    \n\nParams: ({changes_count}, '{created_by}')\n\nTable structure: {self.query('PRAGMA table_info(wscad_comparisons)')}")
            return None

    def add_comparison_to_project(self, project_id, comparison_id, file1_name, file2_name):
        """Add a comparison to a project"""
        try:
            # Generate display name
            display_name = f"{self.clean_filename_for_display(file1_name)} vs {self.clean_filename_for_display(file2_name)}"
            
            # Get next revision number
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
            
            # Update project revision
            if project_comp_id:
                self.update_project_revision(project_id)
            
            return project_comp_id
            
        except Exception as e:
            print(f"Error adding comparison to project: {e}")
            return None

    def get_project_revisions(self, project_id):
        """Get all revisions for a project"""
        try:
            return self.query("""
                SELECT 
                    pc.id,
                    pc.display_name,
                    pc.revision_number,
                    wc.changes_count,
                    wc.comparison_date,
                    wc.id as comparison_id,
                    wc.supabase_saved,
                    wc.created_by
                FROM project_comparisons pc
                JOIN wscad_comparisons wc ON pc.comparison_id = wc.id
                WHERE pc.project_id = ?
                ORDER BY pc.revision_number DESC
            """, (project_id,))
        except Exception as e:
            print(f"Error getting project revisions: {e}")
            return []

    # Supabase sync methods
    def mark_comparison_synced_to_supabase(self, comparison_id, supabase_comparison_id):
        """Mark comparison as synced to Supabase"""
        try:
            return self.execute("""
                UPDATE wscad_comparisons 
                SET supabase_saved = 1, supabase_comparison_id = ?
                WHERE id = ?
            """, (supabase_comparison_id, comparison_id))
        except Exception as e:
            print(f"Error marking comparison as synced: {e}")
            return False

    def mark_project_synced_to_supabase(self, project_id, supabase_project_id):
        """Mark project as synced to Supabase"""
        try:
            return self.execute("""
                UPDATE projects 
                SET supabase_id = ?, sync_status = 'synced', updated_at = ?
                WHERE id = ?
            """, (supabase_project_id, datetime.now(), project_id))
        except Exception as e:
            print(f"Error marking project as synced: {e}")
            return False

    # Activity logging
    def log_activity(self, activity, username, project_id=None, file_info=None, activity_type='general'):
        """Log user activity with improved structure"""
        try:
            file_info_json = json.dumps(file_info) if file_info else None
            activity_id = self.execute("""
                INSERT INTO activity_logs (username, activity, project_id, file_info, activity_type)
                VALUES (?, ?, ?, ?, ?)
            """, (username, activity, project_id, file_info_json, activity_type))
            return activity_id is not None
        except Exception as e:
            print(f"Activity logging error: {e}")
            return False

    def get_activity_logs(self, limit=100, username=None, project_id=None):
        """Get activity logs with filtering"""
        try:
            query = """
                SELECT id, username, activity, timestamp, project_id, file_info, activity_type
                FROM activity_logs
            """
            params = []
            conditions = []
            
            if username:
                conditions.append("username = ?")
                params.append(username)
            
            if project_id:
                conditions.append("project_id = ?")
                params.append(project_id)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            return self.query(query, tuple(params))
        except Exception as e:
            print(f"Error getting activity logs: {e}")
            return []

    # Utility methods
    def clean_filename_for_display(self, filename):
        """Clean filename for display"""
        if not filename:
            return "Unknown File"
            
        # Remove file extension
        name_without_ext = os.path.splitext(filename)[0]
        
        # Remove common revision patterns
        patterns = [
            r'_rev\d+$',      # _rev1, _rev2, etc.
            r'-v\d+$',        # -v1, -v2, etc.
            r'_v\d+$',        # _v1, _v2, etc.
            r'\(\d+\)$',      # (1), (2), etc.
            r'_\d+$',         # _1, _2, etc.
        ]
        
        for pattern in patterns:
            name_without_ext = re.sub(pattern, '', name_without_ext, flags=re.IGNORECASE)

        return name_without_ext.strip()

    def migrate_database_schema(self):
        """Migrate existing database to new schema"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                cursor = conn.cursor()
                
                # Check projects table structure
                cursor.execute("PRAGMA table_info(projects)")
                project_columns = [column[1] for column in cursor.fetchall()]
                
                # Add missing columns to projects table
                if 'updated_at' not in project_columns:
                    cursor.execute("ALTER TABLE projects ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                    print("✅ Added updated_at column to projects table")
                
                if 'project_type' not in project_columns:
                    cursor.execute("ALTER TABLE projects ADD COLUMN project_type TEXT DEFAULT 'wscad'")
                    print("✅ Added project_type column to projects table")
                
                if 'sync_status' not in project_columns:
                    cursor.execute("ALTER TABLE projects ADD COLUMN sync_status TEXT DEFAULT 'pending'")
                    print("✅ Added sync_status column to projects table")
                
                if 'supabase_id' not in project_columns:
                    cursor.execute("ALTER TABLE projects ADD COLUMN supabase_id TEXT")
                    print("✅ Added supabase_id column to projects table")
                
                # Check activity_logs table structure
                cursor.execute("PRAGMA table_info(activity_logs)")
                activity_columns = [column[1] for column in cursor.fetchall()]
                
                # Add missing columns to activity_logs table
                if 'activity_type' not in activity_columns:
                    cursor.execute("ALTER TABLE activity_logs ADD COLUMN activity_type TEXT DEFAULT 'general'")
                    print("✅ Added activity_type column to activity_logs table")
                
                # Update existing projects with updated_at if NULL
                cursor.execute("""
                    UPDATE projects 
                    SET updated_at = created_at 
                    WHERE updated_at IS NULL
                """)
                
                # Migrate wscad_comparisons table to match Supabase schema
                try:
                    # Check if the old columns exist
                    cursor.execute("PRAGMA table_info(wscad_comparisons)")
                    comparison_columns = [column[1] for column in cursor.fetchall()]
                    
                    if 'file_id' in comparison_columns:
                        # Create a temporary table with the new schema
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS wscad_comparisons_new (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                changes_count INTEGER DEFAULT 0,
                                comparison_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                supabase_saved BOOLEAN DEFAULT 0,
                                supabase_comparison_id TEXT,
                                created_by TEXT
                            )
                        """)
                        
                        # Copy data from old table to new table
                        cursor.execute("""
                            INSERT INTO wscad_comparisons_new 
                            (id, changes_count, comparison_date, 
                             supabase_saved, supabase_comparison_id, created_by)
                            SELECT id, changes_count, comparison_date, 
                                   supabase_saved, supabase_comparison_id, created_by
                            FROM wscad_comparisons
                        """)
                        
                        # Drop the old table
                        cursor.execute("DROP TABLE wscad_comparisons")
                        
                        # Rename the new table to the old name
                        cursor.execute("ALTER TABLE wscad_comparisons_new RENAME TO wscad_comparisons")
                        
                        print("✅ Migrated wscad_comparisons table to match Supabase schema")
                except Exception as e:
                    print(f"⚠️ Error migrating wscad_comparisons table: {e}")
                
                conn.commit()
                print("✅ Database schema migration completed")
                return True
                
        except Exception as e:
            print(f"❌ Database migration error: {e}")
            return False
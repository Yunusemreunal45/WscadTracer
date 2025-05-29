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
        self.conn = None  # Add this line
        self._lock = threading.Lock()
        self.setup_database()  # Call setup on initialization

    def setup_database(self):
        """Initialize database tables"""
        try:
            with sqlite3.connect(self.db_file) as self.conn:
                # First drop all tables to start fresh
                self.conn.executescript("""
                    DROP TABLE IF EXISTS activity_logs;
                    DROP TABLE IF EXISTS wscad_file_revisions;
                    DROP TABLE IF EXISTS wscad_comparisons;
                    DROP TABLE IF EXISTS wscad_files;
                    DROP TABLE IF EXISTS project_comparisons;
                    DROP TABLE IF EXISTS projects;
                """)

                # Create tables in correct order with proper constraints
                self.conn.executescript("""
                    CREATE TABLE projects (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT,
                        created_by TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        current_revision INTEGER DEFAULT 0,
                        is_active BOOLEAN DEFAULT 1,
                        supabase_id TEXT,
                        sync_status TEXT DEFAULT 'pending',
                        UNIQUE(name COLLATE NOCASE)
                    );

                    CREATE TABLE activity_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        activity TEXT NOT NULL,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        project_id INTEGER,
                        file_info TEXT,
                        FOREIGN KEY (project_id) REFERENCES projects(id)
                    );

                    CREATE TABLE wscad_files (
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
                        UNIQUE(filepath COLLATE NOCASE)
                    );
                """)

                self.conn.commit()
                print("Database tables created successfully")
                return True
                
        except Exception as e:
            print(f"Database setup error: {e}")
            return False

    def _add_missing_columns(self, cursor):
        """Eksik sütunları mevcut tablolara ekle"""
        try:
            # projects tablosuna supabase_id ve sync_status ekle
            try:
                cursor.execute("ALTER TABLE projects ADD COLUMN supabase_id INTEGER")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var
            
            try:
                cursor.execute("ALTER TABLE projects ADD COLUMN sync_status TEXT DEFAULT 'pending'")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var

            # activity_logs tablosuna project_id ekle
            try:
                cursor.execute("ALTER TABLE activity_logs ADD COLUMN project_id INTEGER")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var

            # wscad_comparisons tablosuna supabase kolonları ekle
            try:
                cursor.execute("ALTER TABLE wscad_comparisons ADD COLUMN supabase_saved BOOLEAN DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var
            
            try:
                cursor.execute("ALTER TABLE wscad_comparisons ADD COLUMN supabase_comparison_id INTEGER")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var

            # project_comparisons tablosuna supabase_saved ekle
            try:
                cursor.execute("ALTER TABLE project_comparisons ADD COLUMN supabase_saved BOOLEAN DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # Sütun zaten var

        except Exception as e:
            print(f"Column migration error: {e}")

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

    # WSCAD BOM dosya yönetimi metodları
    def add_wscad_file(self, filename, filepath, filesize, project_info=None):
        """WSCAD BOM dosyası ekle"""
        try:
            # Proje bilgilerini çıkar
            is_emri_no = ""
            proje_adi = ""
            revizyon_no = ""
            
            if project_info:
                is_emri_no = project_info.get('is_emri_no', '')
                proje_adi = project_info.get('proje_adi', '')
                revizyon_no = project_info.get('revizyon_no', '')

            existing_file = self.query_one("SELECT * FROM wscad_files WHERE filepath = ?", (filepath,))
            
            # Dosya sayısı kontrolü (max 50) - limiti artırdık
            total_files = self.query_one("SELECT COUNT(*) as count FROM wscad_files")
            if total_files and total_files['count'] > 50:
                old_files = self.query("""
                    SELECT id, filepath FROM wscad_files 
                    ORDER BY detected_time ASC 
                    LIMIT ?
                """, (total_files['count'] - 50,))
                
                for old_file in old_files:
                    try:
                        if os.path.exists(old_file['filepath']):
                            os.remove(old_file['filepath'])
                    except:
                        pass  # Dosya silme hatası önemli değil
                    self.execute("DELETE FROM wscad_files WHERE id = ?", (old_file['id'],))
            
            if existing_file:
                # Mevcut dosyayı güncelle
                new_revision = existing_file['current_revision'] + 1
                self.execute("""
                    UPDATE wscad_files 
                    SET current_revision = ?, is_emri_no = ?, proje_adi = ?, revizyon_no = ?, project_info = ?
                    WHERE id = ?
                """, (new_revision, is_emri_no, proje_adi, revizyon_no, json.dumps(project_info) if project_info else None, existing_file['id']))
                
                self.execute("""
                    INSERT INTO wscad_file_revisions (file_id, revision_number, revision_path, is_emri_no, revizyon_no) 
                    VALUES (?, ?, ?, ?, ?)
                """, (existing_file['id'], new_revision, filepath, is_emri_no, revizyon_no))
                
                return existing_file['id']
            else:
                # Yeni dosya ekle
                file_id = self.execute("""
                    INSERT INTO wscad_files (filename, filepath, filesize, detected_time, is_emri_no, proje_adi, revizyon_no, project_info) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (filename, filepath, filesize, datetime.now(), is_emri_no, proje_adi, revizyon_no, json.dumps(project_info) if project_info else None))
                
                if file_id:
                    self.execute("""
                        INSERT INTO wscad_file_revisions (file_id, revision_number, revision_path, is_emri_no, revizyon_no) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (file_id, 1, filepath, is_emri_no, revizyon_no))
                
                return file_id
        except Exception as e:
            print(f"Error adding WSCAD file: {e}")
            return None

    def get_all_wscad_files(self):
        """Tüm WSCAD dosyalarını getir"""
        return self.query("""
            SELECT id, filename, filepath, filesize, detected_time, processed, current_revision,
                   is_emri_no, proje_adi, revizyon_no, project_info
            FROM wscad_files 
            ORDER BY detected_time DESC
        """)

    def save_wscad_comparison_result(self, file_id, rev1_id, rev2_id, changes_count, comparison_date, comparison_summary=None):
        """WSCAD karşılaştırma sonucunu kaydet (yerel)"""
        try:
            comparison_id = self.execute("""
                INSERT INTO wscad_comparisons 
                (file_id, revision1_id, revision2_id, changes_count, comparison_date, comparison_summary)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (file_id, rev1_id, rev2_id, changes_count, comparison_date, comparison_summary))
            
            return comparison_id
        except Exception as e:
            print(f"WSCAD karşılaştırma kaydetme hatası: {e}")
            return None

    # Proje yönetimi metodları
    def create_project(self, name, description, created_by):
        """Create a new project"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO projects (name, description, created_by)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    created_by = excluded.created_by,
                    current_revision = projects.current_revision
                RETURNING id
            """, (name, description, created_by))
            
            project_id = cursor.fetchone()[0]
            self.conn.commit()
            return project_id
            
        except Exception as e:
            print(f"Project creation error: {e}")
            self.conn.rollback()
            return None

    def get_all_projects(self):
        """Get all active projects"""
        try:
            # Önce tabloyu kontrol et
            test = self.query_one("SELECT 1 FROM projects LIMIT 1")
            
            return self.query("""
                SELECT id, name, description, created_by, created_at, is_active, 
                       COALESCE(supabase_id, 0) as supabase_id, 
                       COALESCE(sync_status, 'pending') as sync_status
                FROM projects
                WHERE is_active = 1
                ORDER BY created_at DESC
            """)
        except Exception as e:
            print(f"Error getting projects: {e}")
            return []

    def get_project_by_id(self, project_id):
        """Get project by ID"""
        try:
            result = self.query_one("""
                SELECT id, name, description, created_by, created_at, is_active, 
                       COALESCE(supabase_id, 0) as supabase_id, 
                       COALESCE(sync_status, 'pending') as sync_status
                FROM projects 
                WHERE id = ?
            """, (project_id,))
            return dict(result) if result else None
        except Exception as e:
            print(f"Error getting project by id: {e}")
            return None

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
        try:
            return self.query("""
                SELECT 
                    pc.id,
                    pc.display_name,
                    pc.revision_number,
                    wc.changes_count,
                    wc.comparison_date,
                    wc.comparison_summary,
                    wc.id as comparison_id,
                    COALESCE(wc.supabase_saved, 0) as supabase_saved
                FROM project_comparisons pc
                JOIN wscad_comparisons wc ON pc.comparison_id = wc.id
                WHERE pc.project_id = ?
                ORDER BY pc.revision_number DESC
            """, (project_id,))
        except Exception as e:
            print(f"Error getting project revisions: {e}")
            return []

    def mark_comparison_synced_to_supabase(self, comparison_id, supabase_comparison_id):
        """Mark comparison as synced to Supabase"""
        return self.execute("""
            UPDATE wscad_comparisons 
            SET supabase_saved = 1, supabase_comparison_id = ?
            WHERE id = ?
        """, (supabase_comparison_id, comparison_id))

    def mark_project_synced_to_supabase(self, project_id, supabase_project_id):
        """Mark project as synced to Supabase"""
        return self.execute("""
            UPDATE projects 
            SET supabase_id = ?, sync_status = 'synced'
            WHERE id = ?
        """, (supabase_project_id, project_id))

    # Aktivite log metodları
    def log_activity(self, activity, username, project_id=None, file_info=None):
        """Log user activity with proper connection handling"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                file_info_json = json.dumps(file_info) if file_info else None
                conn.execute("""
                    INSERT INTO activity_logs (username, activity, project_id, file_info)
                    VALUES (?, ?, ?, ?)
                """, (username, activity, project_id, file_info_json))
                conn.commit()
                return True
        except Exception as e:
            print(f"Activity logging error: {e}")
            return False

    def get_activity_logs(self, limit=100):
        """Get activity logs"""
        try:
            return self.query("""
                SELECT id, username, activity, timestamp, 
                       COALESCE(project_id, 0) as project_id, file_info
                FROM activity_logs
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
        except Exception as e:
            print(f"Error getting activity logs: {e}")
            return []

    # Diğer yardımcı metodlar
    def get_comparison_history(self, file_id=None):
        """Get comparison history"""
        try:
            if file_id:
                return self.query("""
                    SELECT 
                        wc.id, 
                        wc.comparison_date, 
                        wc.changes_count, 
                        wf.filename,
                        COALESCE(wc.supabase_saved, 0) as supabase_saved
                    FROM wscad_comparisons wc
                    JOIN wscad_files wf ON wc.file_id = wf.id
                    WHERE wc.file_id = ?
                    ORDER BY wc.comparison_date DESC
                """, (file_id,))
            else:
                return self.query("""
                    SELECT 
                        wc.id, 
                        wc.comparison_date, 
                        wc.changes_count, 
                        wf.filename,
                        COALESCE(wc.supabase_saved, 0) as supabase_saved
                    FROM wscad_comparisons wc
                    JOIN wscad_files wf ON wc.file_id = wf.id
                    ORDER BY wc.comparison_date DESC
                """)
        except Exception as e:
            print(f"Error getting comparison history: {e}")
            return []
        
    def get_recent_wscad_files(self, limit=10):
        """Get recently processed WSCAD files"""
        try:
            query = """
                SELECT f.*, 
                       p.is_emri_no,
                       p.proje_adi
                FROM wscad_files f
                LEFT JOIN wscad_file_metadata p ON f.id = p.file_id
                ORDER BY f.detected_time DESC
                LIMIT ?
            """
            cursor = self.conn.execute(query, (limit,))
            self.conn.commit()
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting recent files: {e}")
            return []

    # Backward compatibility methods (eski kod ile uyumluluk için)
    def add_file(self, filename, filepath, filesize):
        """Backward compatibility - redirects to add_wscad_file"""
        return self.add_wscad_file(filename, filepath, filesize)

    def get_all_files(self):
        """Backward compatibility - redirects to get_all_wscad_files"""
        return self.get_all_wscad_files()

    def save_comparison_result(self, file_id, rev1_id, rev2_id, changes_count, comparison_date, comparison_data=None):
        """Backward compatibility - redirects to save_wscad_comparison_result"""
        summary = f"Changes: {changes_count}" if comparison_data else None
        return self.save_wscad_comparison_result(file_id, rev1_id, rev2_id, changes_count, comparison_date, summary)
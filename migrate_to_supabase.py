import os
import sqlite3
import psycopg2
import psycopg2.extras
import re
import os
from dotenv import load_dotenv
from datetime import datetime
import json
import hashlib
import time

class SupabaseManager:
    """WSCAD BOM karşılaştırma sonuçları için geliştirilmiş Supabase yöneticisi"""
    
    # Singleton instance to ensure we only have one connection manager
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SupabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize Supabase connection manager with improved connection handling"""
        if self._initialized:
            return
            
        load_dotenv()
        self.connection = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5  # Increased from 3 to 5
        self.last_connection_check = 0
        self.connection_check_interval = 10  # Check connection every 10 seconds
        # Use connection string from environment variables
        self.connection_params = {
            'dsn': os.getenv('DATABASE_URL'),  # Use the connection string with pgbouncer
            'connect_timeout': 10
        }
        self._connect()
        self._initialized = True
    
    def _connect(self):
        """Create a new connection to Supabase"""
        try:
            # Close existing connection if it exists
            if self.connection and not self.connection.closed:
                try:
                    self.connection.close()
                except Exception:
                    pass
            
            # Create a new connection using the connection string
            self.connection = psycopg2.connect(
                dsn=os.getenv('DATABASE_URL'),  # Use the connection string with pgbouncer
                connect_timeout=10
            )
            self.connection.autocommit = False
            self.reconnect_attempts = 0
            self.last_connection_check = time.time()
            print("✅ Supabase bağlantısı kuruldu")
            return True
        except Exception as e:
            print(f"❌ Supabase connection error: {e}")
            self.connection = None
            return False

    def close(self):
        """Bağlantıyı kapat"""
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
                self.connection = None
                print("✅ Supabase bağlantısı kapatıldı")
        except Exception as e:
            print(f"⚠️ Supabase connection close error: {e}")
            self.connection = None

    def reconnect(self):
        """Reconnect to Supabase"""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            print(f"🔄 Supabase'e yeniden bağlanılıyor ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
            success = self._connect()
            if success:
                print("✅ Yeniden bağlantı başarılı")
            return success
        else:
            print(f"❌ Maksimum yeniden bağlantı denemesi aşıldı")
            # Reset reconnect attempts after a delay to allow future reconnection attempts
            self.reconnect_attempts = 0
            return False

    def ensure_connection(self):
        """Bağlantının aktif olduğundan emin ol, gerekirse yeniden bağlan"""
        if not self.is_connected():
            return self.reconnect()
        return True

    def is_connected(self):
        """Bağlantının aktif olup olmadığını kontrol et"""
        try:
            if not self.connection or self.connection.closed:
                return False
            
            # Test connection with a simple query
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
        except Exception as e:
            print(f"⚠️ Bağlantı kontrol hatası: {e}")
            return False
            
    def get_connection_status(self):
        """Get detailed connection status information"""
        try:
            if not self.connection or self.connection.closed:
                return {
                    'status': 'disconnected',
                    'message': 'Bağlantı kapalı veya mevcut değil',
                    'version': None
                }
                
            # Test connection and get PostgreSQL version
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute("SELECT version()")
                    version_info = cursor.fetchone()[0]
                    # Extract version number from the version string
                    version_match = re.search(r'PostgreSQL (\d+\.\d+)', version_info)
                    version = version_match.group(1) if version_match else 'Unknown'
                    
                    return {
                        'status': 'connected',
                        'message': 'Bağlantı aktif',
                        'version': version
                    }
                except Exception as e:
                    return {
                        'status': 'error',
                        'message': f'Sorgu hatası: {str(e)}',
                        'version': None
                    }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Bağlantı kontrol hatası: {str(e)}',
                'version': None
            }
            
    def debug_table_structure(self):
        """Check if all required tables exist and have the correct structure"""
        if not self.ensure_connection():
            return False
            
        try:
            with self.connection.cursor() as cursor:
                # Check if required tables exist
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'wscad_%'
                """)
                existing_tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = [
                    'wscad_projects',
                    'wscad_comparison_changes',
                    'wscad_project_statistics'
                ]
                
                missing_tables = [table for table in required_tables if table not in existing_tables]
                
                if missing_tables:
                    print(f"⚠️ Missing tables: {', '.join(missing_tables)}")
                    return False
                    
                # Check if tables have the correct structure
                for table in required_tables:
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = '{table}'
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                    
                    if not columns:
                        print(f"⚠️ Table {table} exists but has no columns")
                        return False
                        
                return True
        except Exception as e:
            print(f"❌ Error checking table structure: {str(e)}")
            return False

    def setup_wscad_tables(self):
        """Setup WSCAD specific tables in Supabase"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False

            with self.connection.cursor() as cursor:
                # First drop all existing tables and constraints
                # Explicitly drop the constraint first
                try:
                    cursor.execute("""
                        ALTER TABLE IF EXISTS wscad_quantity_changes DROP CONSTRAINT IF EXISTS valid_quantity_type;
                    """)
                    self.connection.commit()
                except Exception as e:
                    print(f"Error dropping constraint: {e}")
                    self.connection.rollback()
                
                # Then drop the tables
                cursor.execute("""
                    -- Drop tables with CASCADE
                    DROP TABLE IF EXISTS wscad_quantity_changes CASCADE;
                    DROP TABLE IF EXISTS wscad_comparison_changes CASCADE;
                    DROP TABLE IF EXISTS wscad_project_comparisons CASCADE;
                    DROP TABLE IF EXISTS wscad_project_statistics CASCADE;
                    DROP TABLE IF EXISTS wscad_projects CASCADE;
                """)

                # Create tables in single transaction - Simplified schema
                cursor.execute("""
                    -- Projects table
                    CREATE TABLE wscad_projects (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        description TEXT,
                        created_by VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        supabase_id VARCHAR(255) UNIQUE,
                        sync_status VARCHAR(255) DEFAULT 'pending',
                        project_type VARCHAR(255) DEFAULT 'wscad',
                        current_revision INTEGER DEFAULT 0,
                        sqlite_project_id INTEGER
                    );

                    -- Project Comparisons table
                    CREATE TABLE wscad_project_comparisons (
                        id SERIAL PRIMARY KEY,
                        project_id INTEGER REFERENCES wscad_projects(id),
                        comparison_id INTEGER,
                        display_name TEXT,
                        revision_number INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        changes_count INTEGER DEFAULT 0,
                        file1_name TEXT,
                        file2_name TEXT,
                        comparison_summary JSONB,
                        status TEXT DEFAULT 'active',
                        created_by TEXT,
                        comparison_hash TEXT
                    );

                    -- Comparison Changes table (No constraints)
                    CREATE TABLE wscad_comparison_changes (
                        id SERIAL PRIMARY KEY,
                        project_comparison_id INTEGER REFERENCES wscad_project_comparisons(id),
                        change_type VARCHAR(50) NOT NULL,
                        poz_no TEXT,
                        parca_no TEXT,
                        parca_adi TEXT,
                        column_name TEXT,
                        old_value TEXT,
                        new_value TEXT,
                        severity TEXT DEFAULT 'medium',
                        description TEXT,
                        modified_by TEXT,
                        modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    -- Quantity Changes table (No constraints)
                    CREATE TABLE wscad_quantity_changes (
                        id SERIAL PRIMARY KEY,
                        project_comparison_id INTEGER REFERENCES wscad_project_comparisons(id),
                        poz_no TEXT,
                        parca_no TEXT,
                        parca_adi TEXT,
                        old_quantity NUMERIC,
                        new_quantity NUMERIC,
                        quantity_change_type VARCHAR(50) NOT NULL,
                        percentage_change NUMERIC,
                        impact_description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    -- Project Statistics table
                    CREATE TABLE wscad_project_statistics (
                        id SERIAL PRIMARY KEY,
                        project_id INTEGER REFERENCES wscad_projects(id) UNIQUE,
                        total_comparisons INTEGER DEFAULT 0,
                        total_changes INTEGER DEFAULT 0,
                        total_critical_changes INTEGER DEFAULT 0,
                        total_added_items INTEGER DEFAULT 0,
                        total_removed_items INTEGER DEFAULT 0,
                        total_quantity_changes INTEGER DEFAULT 0,
                        last_comparison_date TIMESTAMP,
                        average_changes_per_comparison NUMERIC DEFAULT 0,
                        most_active_contributor TEXT,
                        trend_analysis JSONB,
                        performance_metrics JSONB,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    -- Create indexes for better performance
                    CREATE INDEX idx_wscad_projects_name ON wscad_projects(name);
                    CREATE INDEX idx_wscad_projects_sync_status ON wscad_projects(sync_status);
                    
                    CREATE INDEX idx_wscad_comparisons_project ON wscad_project_comparisons(project_id);
                    CREATE INDEX idx_wscad_comparisons_revision ON wscad_project_comparisons(revision_number);
                    
                    CREATE INDEX idx_wscad_changes_comparison ON wscad_comparison_changes(project_comparison_id);
                    CREATE INDEX idx_wscad_changes_type ON wscad_comparison_changes(change_type);
                    CREATE INDEX idx_wscad_changes_poz ON wscad_comparison_changes(poz_no);
                    
                    CREATE INDEX idx_wscad_quantity_changes_comparison ON wscad_quantity_changes(project_comparison_id);
                    CREATE INDEX idx_wscad_quantity_changes_poz ON wscad_quantity_changes(poz_no);
                """)

                # Verify table creation
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'wscad_%'
                """)
                created_tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = [
                    'wscad_projects',
                    'wscad_project_comparisons',
                    'wscad_comparison_changes',
                    'wscad_quantity_changes',
                    'wscad_project_statistics'
                ]
                
                if not all(table in created_tables for table in required_tables):
                    missing_tables = [table for table in required_tables if table not in created_tables]
                    print(f"❌ Bazı tablolar oluşturulamadı: {', '.join(missing_tables)}")
                    self.connection.rollback()
                    return False

                self.connection.commit()
                print("✅ WSCAD tables created successfully")
                return True

        except Exception as e:
            print(f"❌ Table setup error: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return False

    def create_wscad_project(self, name, description, created_by, sqlite_project_id=None):
        """Create a new WSCAD project with improved connection handling"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot create project: Supabase connection failed")
                return None

            with self.connection.cursor() as cursor:
                try:
                    # Önce projenin var olup olmadığını kontrol et
                    cursor.execute("""
                        SELECT id FROM wscad_projects 
                        WHERE name = %s AND created_by = %s
                    """, (name, created_by))
                    
                    existing_project = cursor.fetchone()
                    
                    if existing_project:
                        project_id = existing_project[0]
                        # Projeyi güncelle
                        cursor.execute("""
                            UPDATE wscad_projects 
                            SET description = %s,
                                updated_at = CURRENT_TIMESTAMP,
                                sqlite_project_id = COALESCE(%s, sqlite_project_id)
                            WHERE id = %s
                            RETURNING id
                        """, (description, sqlite_project_id, project_id))
                        
                        project_id = cursor.fetchone()[0]
                        self.connection.commit()
                        print(f"✅ Project updated successfully: {name} (ID: {project_id})")
                        return project_id
                    
                    # Yeni proje oluştur
                    cursor.execute("""
                        INSERT INTO wscad_projects 
                        (name, description, created_by, sqlite_project_id) 
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                    """, (name, description, created_by, sqlite_project_id))
                    
                    project_id = cursor.fetchone()[0]
                    self.connection.commit()
                    print(f"✅ New project created: {name} (ID: {project_id})")
                    return project_id
                except Exception as e:
                    self.connection.rollback()
                    print(f"❌ Database operation error: {str(e)}")
                    # Try to reconnect on database errors
                    self.reconnect()
                    return None
                
        except Exception as e:
            print(f"❌ Project creation error: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            # Try to reconnect on any error
            self.reconnect()
            return None
    
    def get_wscad_projects(self, created_by=None):
        """Tüm WSCAD projelerini getir - filtreleme desteği ile"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get projects: Supabase connection failed")
                return []

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                query = """
                    SELECT 
                        wp.*,
                        wps.total_comparisons,
                        wps.total_changes,
                        wps.total_critical_changes,
                        wps.total_added_items,
                        wps.total_removed_items,
                        wps.last_comparison_date,
                        wps.average_changes_per_comparison,
                        wps.most_active_contributor
                    FROM 
                        wscad_projects wp
                    LEFT JOIN 
                        wscad_project_statistics wps ON wp.id = wps.project_id
                    WHERE 
                        wp.is_active = TRUE
                """
                params = []
                
                if created_by:
                    query += " AND wp.created_by = %s"
                    params.append(created_by)
                
                query += " ORDER BY wp.updated_at DESC"
                
                cursor.execute(query, tuple(params) if params else None)
                return cursor.fetchall()
            
            except Exception as e:
                print(f"❌ WSCAD proje listesi alma hatası: {e}")
                # Try to reconnect on database errors
                self.reconnect()
                return []
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
            
        except Exception as e:
            print(f"❌ WSCAD proje listesi alma hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return []
    
    def _get_change_type(self, change):
        """Map change types to valid constraint values - FIXED"""
        base_type = change.get('change_type', '').lower()
        column = change.get('column', '').lower()
        
        # Handle quantity changes
        if ('quantity' in base_type or 'adet' in column or 'miktar' in column or 
            'toplam' in column or any(qty_word in column for qty_word in ['adet', 'quantity', 'miktar'])):
            
            old_val = self._safe_float(change.get('value1', 0))
            new_val = self._safe_float(change.get('value2', 0))
            
            if old_val is None or new_val is None:
                return 'modified'
                
            if new_val > old_val:
                return 'quantity_increased'
            elif new_val < old_val:
                return 'quantity_decreased'
            return 'quantity_changed'
        
        # Map other types to valid constraint values
        type_mapping = {
            'added': 'added',
            'removed': 'removed',
            'changed': 'modified',
            'modified': 'modified',
            'updated': 'modified',
            'structural': 'structural',
            'component': 'component',
            'description': 'description'
        }
        
        return type_mapping.get(base_type, 'modified')

    def save_wscad_comparison_to_project(self, project_id, comparison_data, file1_name, file2_name, 
                                         file1_info=None, file2_info=None, created_by=None):
        """WSCAD karşılaştırma sonucunu projeye kaydet - optimize edilmiş"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot save comparison: Supabase connection failed")
                return None

            cursor = None
            try:
                cursor = self.connection.cursor()
                
                # Önce projenin var olduğunu kontrol et
                cursor.execute("""
                    SELECT id, name, description 
                    FROM wscad_projects 
                    WHERE id = %s
                """, (project_id,))
                
                project = cursor.fetchone()
                if not project:
                    print(f"❌ Project with ID {project_id} not found")
                    return None
                
                # Revizyon numarasını hesapla
                cursor.execute("""
                    SELECT MAX(revision_number) 
                    FROM wscad_project_comparisons 
                    WHERE project_id = %s
                """, (project_id,))
                
                max_revision = cursor.fetchone()[0] or 0
                next_revision = max_revision + 1
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                return None
            
            # Dosya bilgilerini güvenli şekilde hazırla
            try:
                def safe_get(obj, key, default=''):
                    if isinstance(obj, dict):
                        return obj.get(key, default)
                    return default
                
                file1_is_emri = safe_get(file1_info, 'is_emri_no')
                file1_proje = safe_get(file1_info, 'proje_adi')
                file1_rev = safe_get(file1_info, 'revizyon_no')
                file2_is_emri = safe_get(file2_info, 'is_emri_no')
                file2_proje = safe_get(file2_info, 'proje_adi')
                file2_rev = safe_get(file2_info, 'revizyon_no')
                
                # Karşılaştırma başlığını oluştur
                display_name = f"Rev {next_revision}: {os.path.basename(file1_name)} → {os.path.basename(file2_name)}"
                
                # Karşılaştırma hash'i oluştur
                comparison_hash = hashlib.md5(f"{file1_name}{file2_name}{len(comparison_data)}{datetime.now().isoformat()}".encode()).hexdigest()

                # Karşılaştırma özetini oluştur
                summary_stats = self._generate_comparison_summary(comparison_data)
                comparison_summary = json.dumps(summary_stats, ensure_ascii=False)
                # Ana karşılaştırma kaydı
                cursor.execute("""
                    INSERT INTO wscad_project_comparisons 
                    (project_id, display_name, revision_number, file1_name, file2_name, 
                     changes_count, created_by, comparison_hash, comparison_summary,
                     status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    project_id, display_name, next_revision, file1_name, file2_name,
                    len(comparison_data), created_by, comparison_hash, comparison_summary,
                    'active'
                ))
                
                comparison_id = cursor.fetchone()[0]
                
                # Değişiklikleri kaydet
                if comparison_data:
                    changes_to_insert = []

                    # FIXED: Change type mapping method
                    for change in comparison_data:
                        change_type = self._get_change_type(change)  # Use self reference
                        severity = self._determine_change_severity(change)
                        # impact_level ve change_category artık kullanılmıyor
                        # impact_level = self._determine_impact_level(change)
                        
                        changes_to_insert.append((
                            comparison_id,
                            change_type,  # This will now be a valid type
                            change.get('poz_no', '')[:50],
                            change.get('parca_no', '')[:100],
                            change.get('parca_adi', '')[:200],
                            change.get('column', '')[:50],
                            str(change.get('value1', ''))[:500],
                            str(change.get('value2', ''))[:500],
                            severity,
                            change.get('description', '')[:1000],
                            created_by
                        ))

                        # Miktar değişikliği işleme kodu kaldırıldı

                    # Toplu insert işlemleri
                    if changes_to_insert:
                        psycopg2.extras.execute_batch(cursor, """
                            INSERT INTO wscad_comparison_changes 
                            (project_comparison_id, change_type, poz_no, parca_no, parca_adi,
                             column_name, old_value, new_value, severity, description, modified_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, changes_to_insert, page_size=1000)

                    # wscad_quantity_changes tablosu kaldırıldı

                # İstatistikleri güncelle
                self._update_project_statistics(cursor, project_id, len(comparison_data), created_by, comparison_data)

                # Proje updated_at zamanını ve current_revision'ını güncelle
                cursor.execute("""
                    UPDATE wscad_projects 
                    SET updated_at = CURRENT_TIMESTAMP, current_revision = %s
                    WHERE id = %s
                """, (next_revision, project_id))

                self.connection.commit()
                print(f"✅ WSCAD comparison saved to Supabase: Rev {next_revision} (ID: {comparison_id})")
                return comparison_id

            except psycopg2.Error as e:
                print(f"❌ Database error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return None
            except Exception as e:
                print(f"❌ Comparison data processing error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                return None
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()

        except Exception as e:
            print(f"❌ Supabase kaydetme hatası: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            # Try to reconnect on any error
            self.reconnect()
            return None
    
    def _generate_comparison_summary(self, comparison_data):
        """Karşılaştırma özeti oluştur"""
        summary = {
            'total_changes': len(comparison_data),
            'by_type': {},
            'by_severity': {},
            'by_category': {},
            'structural_changes': 0,
            'critical_poz_numbers': []
        }
        
        for change in comparison_data:
            change_type = change.get('change_type', 'unknown')
            severity = self._determine_change_severity(change)
            # category = self._determine_change_category(change) # artık kullanılmıyor
            
            # Type statistics
            summary['by_type'][change_type] = summary['by_type'].get(change_type, 0) + 1
            
            # Severity statistics
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # Category statistics artık kullanılmıyor
            # summary['by_category'][category] = summary['by_category'].get(category, 0) + 1
            
            # Quantity changes tracking removed
            
            # Structural changes
            if change.get('type') == 'structure':
                summary['structural_changes'] += 1
            
            # Critical POZ numbers
            if severity == 'high' and change.get('poz_no'):
                poz_no = change.get('poz_no')
                if poz_no not in summary['critical_poz_numbers']:
                    summary['critical_poz_numbers'].append(poz_no)
        
        return summary
    
    def _determine_change_severity(self, change):
        """Değişiklik şiddetini belirle"""
        change_type = change.get('change_type', '')
        column = change.get('column', '').lower()
        
        # Kritik değişiklikler
        if change_type in ['added', 'removed']:
            return 'high'
        elif 'quantity' in change_type:
            return 'high'
        elif any(critical_col in column for critical_col in ['poz_no', 'parca_no', 'poz no', 'parca no']):
            return 'high'
        # Orta seviye değişiklikler
        elif any(medium_col in column for medium_col in ['parca_adi', 'toplam_adet', 'parca adi', 'toplam adet']):
            return 'medium'
        # Düşük seviye değişiklikler
        else:
            return 'low'
    
    def _determine_impact_level(self, change):
        """Değişikliğin etki seviyesini belirle"""
        severity = self._determine_change_severity(change)
        change_type = change.get('change_type', '')
        
        if severity == 'high' and change_type in ['added', 'removed']:
            return 'critical'
        elif severity == 'high':
            return 'high'
        elif severity == 'medium':
            return 'normal'
        else:
            return 'low'
    
    # Miktar değişikliği ile ilgili metotlar kaldırıldı
    
    def _safe_float(self, value):
        """Güvenli float dönüşümü"""
        try:
            if value is None or value == '':
                return 0.0
            return float(str(value).replace(',', '.'))
        except:
            return None
    
    def _update_project_statistics(self, cursor, project_id, changes_count, created_by, comparison_data):
        """Proje istatistiklerini güncelle - geliştirilmiş"""
        try:
            # Calculate detailed statistics
            critical_changes = sum(1 for change in comparison_data 
                                 if self._determine_change_severity(change) == 'high')
            added_items = sum(1 for change in comparison_data 
                            if change.get('change_type') == 'added')
            removed_items = sum(1 for change in comparison_data 
                              if change.get('change_type') == 'removed')
            # quantity_changes kaldırıldı
            quantity_changes = 0
            
            # Trend analysis data
            trend_data = {
                'last_comparison': {
                    'changes_count': changes_count,
                    'critical_changes': critical_changes,
                    'date': datetime.now().isoformat()
                }
            }
            
            # Performance metrics
            performance_metrics = {
                'change_velocity': changes_count,
                'quality_impact': critical_changes / max(changes_count, 1),
                'stability_index': 1 - (critical_changes / max(changes_count, 1))
            }
            
            # Update statistics
            # Simplified SQL query with explicit column listing and parameter count matching
            cursor.execute("""
                INSERT INTO wscad_project_statistics (
                    project_id, total_comparisons, total_changes,
                    total_critical_changes, total_added_items, total_removed_items,
                    total_quantity_changes, last_comparison_date, most_active_contributor,
                    trend_analysis, performance_metrics, updated_at
                ) VALUES (
                    %s, 1, %s, %s, %s, %s, 0, CURRENT_TIMESTAMP, %s, %s, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (project_id) DO UPDATE SET
                    total_comparisons = wscad_project_statistics.total_comparisons + 1,
                    total_changes = wscad_project_statistics.total_changes + %s,
                    total_critical_changes = wscad_project_statistics.total_critical_changes + %s,
                    total_added_items = wscad_project_statistics.total_added_items + %s,
                    total_removed_items = wscad_project_statistics.total_removed_items + %s,
                    total_quantity_changes = wscad_project_statistics.total_quantity_changes,
                    last_comparison_date = CURRENT_TIMESTAMP,
                    most_active_contributor = %s,
                    average_changes_per_comparison = (wscad_project_statistics.total_changes + %s) / 
                                                   (wscad_project_statistics.total_comparisons + 1),
                    trend_analysis = %s,
                    performance_metrics = %s,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                project_id, changes_count, critical_changes, added_items, removed_items,
                created_by, json.dumps(trend_data), json.dumps(performance_metrics),
                changes_count, critical_changes, added_items, removed_items,
                created_by, changes_count, json.dumps(trend_data), json.dumps(performance_metrics)
            ))
            
        except Exception as e:
            print(f"⚠️ Statistics update error: {e}")
    
    def get_wscad_project_comparisons(self, project_id, limit=50):
        """WSCAD projesinin tüm karşılaştırmalarını getir - sayfalama ile"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get project comparisons: Supabase connection failed")
                return []

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT id, display_name as comparison_title, file1_name, file2_name, 
                           changes_count, revision_number, created_by, created_at,
                           comparison_summary, status
                    FROM wscad_project_comparisons
                    WHERE project_id = %s AND status = 'active'
                    ORDER BY revision_number DESC
                    LIMIT %s
                """, (project_id, limit))
                
                return cursor.fetchall()
            except Exception as e:
                print(f"❌ WSCAD proje karşılaştırmaları alma hatası: {e}")
                # Try to reconnect on database errors
                self.reconnect()
                return []
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
            
        except Exception as e:
            print(f"❌ WSCAD proje karşılaştırmaları alma hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return []
    
    def get_wscad_comparison_details(self, comparison_id):
        """WSCAD karşılaştırma detaylarını getir"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get comparison details: Supabase connection failed")
                return None
            
            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Get the main comparison record
                cursor.execute("""
                    SELECT * FROM wscad_project_comparisons WHERE id = %s
                """, (comparison_id,))
                
                comparison = cursor.fetchone()
                if not comparison:
                    return None
                
                # Get the changes
                cursor.execute("""
                    SELECT * FROM wscad_comparison_changes 
                    WHERE project_comparison_id = %s 
                    ORDER BY severity, poz_no, id
                    LIMIT 1000
                """, (comparison_id,))
                
                changes = cursor.fetchall()
                
                # Get summary statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_changes,
                        SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as critical_changes,
                        SUM(CASE WHEN change_type = 'added' THEN 1 ELSE 0 END) as added_items,
                        SUM(CASE WHEN change_type = 'removed' THEN 1 ELSE 0 END) as removed_items
                    FROM wscad_comparison_changes 
                    WHERE project_comparison_id = %s
                """, (comparison_id,))
                
                stats = cursor.fetchone()
                
                # Combine all data
                result = dict(comparison)
                result['changes'] = changes
                result['stats'] = stats
                
                return result
                
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                # Rollback transaction on error
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return None
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
        
        except Exception as e:
            print(f"❌ WSCAD karşılaştırma detayları alma hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return None
    
    def get_wscad_project_statistics(self, project_id):
        """WSCAD proje istatistiklerini getir - geliştirilmiş"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get project statistics: Supabase connection failed")
                return None

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Genel istatistikler
                cursor.execute("""
                    SELECT * FROM wscad_project_statistics WHERE project_id = %s
                """, (project_id,))
                general_stats = cursor.fetchone()
                
                # Değişiklik türü istatistikleri - optimize edilmiş
                cursor.execute("""
                    SELECT 
                        change_type,
                        severity,
                        COUNT(*) as count
                    FROM wscad_comparison_changes
                    WHERE project_comparison_id IN (
                        SELECT id FROM wscad_project_comparisons WHERE project_id = %s
                    )
                    GROUP BY change_type, severity
                    ORDER BY count DESC
                    LIMIT 20
                """, (project_id,))
                change_stats = cursor.fetchall()
                
                # En çok değişen POZ NO'lar
                cursor.execute("""
                    SELECT 
                        poz_no,
                        parca_adi,
                        COUNT(*) as change_count,
                        COUNT(CASE WHEN severity = 'high' THEN 1 END) as critical_changes,
                        MAX(wcc.modified_date) as last_change_date
                    FROM wscad_comparison_changes wcc
                    JOIN wscad_project_comparisons wpc ON wcc.project_comparison_id = wpc.id
                    WHERE wpc.project_id = %s AND poz_no IS NOT NULL AND poz_no != ''
                    GROUP BY poz_no, parca_adi
                    ORDER BY change_count DESC
                    LIMIT 10
                """, (project_id,))
                poz_stats = cursor.fetchall()
                
                # Zaman bazlı trend analizi
                cursor.execute("""
                    SELECT 
                        DATE_TRUNC('week', created_at) as week,
                        COUNT(*) as comparisons,
                        SUM(changes_count) as total_changes,
                        AVG(changes_count) as avg_changes_per_comparison
                    FROM wscad_project_comparisons
                    WHERE project_id = %s
                    GROUP BY DATE_TRUNC('week', created_at)
                    ORDER BY week DESC
                    LIMIT 12
                """, (project_id,))
                trend_stats = cursor.fetchall()
                
                # Revizyon bazlı analiz
                cursor.execute("""
                    SELECT 
                        revision_number,
                        display_name,
                        changes_count,
                        created_at,
                        created_by,
                        (SELECT COUNT(*) FROM wscad_comparison_changes 
                         WHERE project_comparison_id = wpc.id AND severity = 'high') as critical_changes
                    FROM wscad_project_comparisons wpc
                    WHERE project_id = %s
                    ORDER BY revision_number DESC
                    LIMIT 10
                """, (project_id,))
                revision_stats = cursor.fetchall()
                
                # Combine all statistics
                result = {
                    'general': general_stats,
                    'changes': change_stats,
                    'top_changed_items': poz_stats,
                    'trends': trend_stats,
                    'revisions': revision_stats
                }
                
                return result
                
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return None
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ WSCAD proje istatistikleri alma hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return None
    
    def get_project_revision_history(self, project_id, limit=20):
        """Proje revizyon geçmişini detaylı olarak getir"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get revision history: Supabase connection failed")
                return []

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT 
                        wpc.id,
                        wpc.project_id,
                        wpc.display_name,
                        wpc.file1_name,
                        wpc.file2_name,
                        wpc.changes_count,
                        wpc.revision_number,
                        wpc.created_by,
                        wpc.created_at,
                        wpc.comparison_summary,
                        wpc.status,
                        COUNT(wcc.id) as detailed_changes_count,
                        COUNT(CASE WHEN wcc.severity = 'high' THEN 1 END) as critical_changes,
                        COUNT(CASE WHEN wcc.severity = 'medium' THEN 1 END) as medium_changes,
                        COUNT(CASE WHEN wcc.severity = 'low' THEN 1 END) as low_changes
                    FROM wscad_project_comparisons wpc
                    LEFT JOIN wscad_comparison_changes wcc ON wpc.id = wcc.project_comparison_id
                    WHERE wpc.project_id = %s AND wpc.status = 'active'
                    GROUP BY wpc.id
                    ORDER BY wpc.revision_number DESC
                    LIMIT %s
                """, (project_id, limit))
                
                return cursor.fetchall()
                
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return []
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ Revizyon geçmişi alma hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return []
    
    def get_recent_comparisons(self, limit=20, created_by=None):
        """Son karşılaştırmaları getir"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot get recent comparisons: Supabase connection failed")
                return []

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                query = """
                    SELECT 
                        wpc.id,
                        wp.name as project_name,
                        wpc.display_name,
                        wpc.revision_number,
                        wpc.changes_count,
                        wpc.created_by,
                        wpc.created_at,
                        wpc.file1_name,
                        wpc.file2_name
                    FROM wscad_project_comparisons wpc
                    JOIN wscad_projects wp ON wpc.project_id = wp.id
                    WHERE wp.is_active = TRUE AND wpc.status = 'active'
                """
                params = []
                
                if created_by:
                    query += " AND wpc.created_by = %s"
                    params.append(created_by)
                
                query += " ORDER BY wpc.created_at DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, tuple(params))
                return cursor.fetchall()
                
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return []
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ Recent comparisons hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return []
    
    def search_projects(self, search_term, created_by=None):
        """Proje arama"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot search projects: Supabase connection failed")
                return []

            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                query = """
                    SELECT wp.*, wps.total_comparisons, wps.total_changes, wps.last_comparison_date
                    FROM wscad_projects wp
                    LEFT JOIN wscad_project_statistics wps ON wp.id = wps.project_id
                    WHERE wp.is_active = TRUE 
                    AND (LOWER(wp.name) LIKE LOWER(%s) OR LOWER(wp.description) LIKE LOWER(%s))
                """
                params = [f"%{search_term}%", f"%{search_term}%"]
                
                if created_by:
                    query += " AND wp.created_by = %s"
                    params.append(created_by)
                
                query += " ORDER BY wp.updated_at DESC LIMIT 50"
                
                cursor.execute(query, tuple(params))
                return cursor.fetchall()
                
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return []
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ Project search hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return []
    
    def delete_project(self, project_id, created_by=None):
        """Projeyi soft delete et"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot delete project: Supabase connection failed")
                return False

            cursor = None
            try:
                cursor = self.connection.cursor()
                
                query = "UPDATE wscad_projects SET is_active = FALSE WHERE id = %s"
                params = [project_id]
                
                if created_by:
                    query += " AND created_by = %s"
                    params.append(created_by)
                
                cursor.execute(query, tuple(params))
                affected_rows = cursor.rowcount
                
                self.connection.commit()
                
                if affected_rows > 0:
                    print(f"✅ Project {project_id} deleted")
                    return True
                else:
                    print(f"⚠️ Project {project_id} not found or access denied")
                    return False
                    
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return False
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ Project deletion error: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return False
    
    def archive_revision(self, comparison_id, created_by=None):
        """Revizyonu arşivle"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot archive revision: Supabase connection failed")
                return False

            cursor = None
            try:
                cursor = self.connection.cursor()
                
                query = "UPDATE wscad_project_comparisons SET status = 'archived' WHERE id = %s"
                params = [comparison_id]
                
                if created_by:
                    query += " AND created_by = %s"
                    params.append(created_by)
                
                cursor.execute(query, tuple(params))
                affected_rows = cursor.rowcount
                
                self.connection.commit()
                
                return affected_rows > 0
                    
            except Exception as e:
                print(f"❌ Database query error: {str(e)}")
                if self.connection and not self.connection.closed:
                    self.connection.rollback()
                # Try to reconnect on database errors
                self.reconnect()
                return False
            finally:
                # Always close the cursor to prevent resource leaks
                if cursor and not cursor.closed:
                    cursor.close()
                    
        except Exception as e:
            print(f"❌ Revision archive error: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return False
    
    def sync_project_from_sqlite(self, sqlite_project):
        """SQLite'dan projeyi Supabase'e senkronize et"""
        try:
            # Ensure connection is active before proceeding
            if not self.ensure_connection():
                print("❌ Cannot sync project: Supabase connection failed")
                return None
                
            try:
                supabase_project_id = self.create_wscad_project(
                    sqlite_project['name'],
                    sqlite_project['description'],
                    sqlite_project['created_by'],
                    sqlite_project['id']
                )
                return supabase_project_id
            except Exception as e:
                print(f"❌ Database operation error: {str(e)}")
                # Try to reconnect on database errors
                self.reconnect()
                return None
                
        except Exception as e:
            print(f"❌ Proje senkronizasyon hatası: {e}")
            # Try to reconnect on any error
            self.reconnect()
            return None
    
    def get_connection_status(self):
        """Bağlantı durumunu kontrol et"""
        try:
            if not self.connection:
                return {'status': 'disconnected', 'message': 'No connection'}
            
            if self.connection.closed:
                return {'status': 'disconnected', 'message': 'Connection closed'}
            
            # Test query
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                
            return {
                'status': 'connected', 
                'message': 'Connection active',
                'version': version
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def debug_table_structure(self):
        """Supabase tablo yapısını kontrol et ve debug bilgisi ver"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False
                
            with self.connection.cursor() as cursor:
                # Tüm WSCAD tablolarının yapısını kontrol et
                tables = ['wscad_projects', 'wscad_project_comparisons', 'wscad_comparison_changes', 
                         'wscad_quantity_changes', 'wscad_project_statistics']
                
                for table_name in tables:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """, (table_name,))
                    columns = cursor.fetchall()
                    
                    print(f"🔍 {table_name.upper()} Tablo Yapısı:")
                    print("-" * 80)
                    for col in columns:
                        print(f"  {col[0]:<25} | {col[1]:<20} | Nullable: {col[2]} | Default: {col[3]}")
                    print("-" * 80)
                    print()
                
                # View'ları kontrol et
                cursor.execute("""
                    SELECT table_name FROM information_schema.views 
                    WHERE table_name LIKE 'wscad_%'
                """)
                views = cursor.fetchall()
                
                print("📊 WSCAD Views:")
                for view in views:
                    print(f"  - {view[0]}")
                
                return True
                    
        except Exception as e:
            print(f"❌ Tablo yapısı kontrol hatası: {e}")
            return False

    def fix_table_structure(self):
        """Eksik sütunları ve yapıları otomatik düzelt"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False
                
            print("🔧 WSCAD tablo yapısı düzeltiliyor...")
            
            # Tabloları yeniden oluştur
            return self.setup_wscad_tables()
                
        except Exception as e:
            print(f"❌ Tablo düzeltme hatası: {e}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return False

    def export_project_data(self, project_id, format='json'):
        """Proje verilerini export et (JSON/CSV)"""
        try:
            if not self.is_connected() and not self.reconnect():
                return None

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Proje bilgilerini al
            cursor.execute("SELECT * FROM wscad_projects WHERE id = %s", (project_id,))
            project = cursor.fetchone()
            
            if not project:
                return None
            
            # Revizyonları al
            revisions = self.get_project_revision_history(project_id, limit=100)
            
            # İstatistikleri al
            statistics = self.get_wscad_project_statistics(project_id)
            
            export_data = {
                'project': dict(project),
                'revisions': [dict(rev) for rev in revisions],
                'statistics': statistics,
                'export_date': datetime.now().isoformat(),
                'format_version': '1.0'
            }
            
            if format.lower() == 'json':
                return json.dumps(export_data, ensure_ascii=False, indent=2, default=str)
            else:
                # CSV format için pandas kullanabilir
                return export_data
                
        except Exception as e:
            print(f"❌ Export hatası: {e}")
            return None

    def get_dashboard_data(self, created_by=None, days=30):
        """Dashboard için özet veri getir"""
        try:
            if not self.is_connected() and not self.reconnect():
                return None

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Son N gün içindeki aktiviteler
            date_filter = f"AND wpc.created_at >= CURRENT_DATE - INTERVAL '{days} days'" if days else ""
            user_filter = f"AND wp.created_by = '{created_by}'" if created_by else ""
            
            # Genel istatistikler
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT wp.id) as total_projects,
                    COUNT(DISTINCT wpc.id) as total_revisions,
                    COALESCE(SUM(wpc.changes_count), 0) as total_changes,
                    COUNT(DISTINCT wpc.created_by) as active_users
                FROM wscad_projects wp
                LEFT JOIN wscad_project_comparisons wpc ON wp.id = wpc.project_id
                WHERE wp.is_active = TRUE {user_filter} {date_filter}
            """)
            general_stats = cursor.fetchone()
            
            # En aktif projeler
            cursor.execute(f"""
                SELECT 
                    wp.name,
                    COUNT(wpc.id) as revision_count,
                    SUM(wpc.changes_count) as total_changes,
                    MAX(wpc.created_at) as last_activity
                FROM wscad_projects wp
                JOIN wscad_project_comparisons wpc ON wp.id = wpc.project_id
                WHERE wp.is_active = TRUE {user_filter} {date_filter}
                GROUP BY wp.id, wp.name
                ORDER BY revision_count DESC
                LIMIT 10
            """)
            active_projects = cursor.fetchall()
            
            # Günlük aktivite trendi
            cursor.execute(f"""
                SELECT 
                    DATE(wpc.created_at) as date,
                    COUNT(*) as revisions,
                    SUM(wpc.changes_count) as changes
                FROM wscad_project_comparisons wpc
                JOIN wscad_projects wp ON wpc.project_id = wp.id
                WHERE wp.is_active = TRUE {user_filter} {date_filter}
                GROUP BY DATE(wpc.created_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            daily_activity = cursor.fetchall()
            
            return {
                'general': dict(general_stats) if general_stats else {},
                'active_projects': [dict(proj) for proj in active_projects],
                'daily_activity': [dict(day) for day in daily_activity],
                'period_days': days,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Dashboard data hatası: {e}")
            return None


def get_sqlite_connection(db_file="wscad_comparison.db"):
    """SQLite veritabanı bağlantısı al"""
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        print(f"✅ SQLite veritabanına bağlanıldı: {db_file}")
        return conn
    except Exception as e:
        print(f"❌ SQLite bağlantı hatası: {e}")
        return None

def migrate_wscad_projects_to_supabase(sqlite_db, supabase_manager):
    """WSCAD projelerini SQLite'dan Supabase'e migrate et - geliştirilmiş"""
    try:
        sqlite_conn = get_sqlite_connection(sqlite_db)
        if not sqlite_conn:
            return False
        
        cursor = sqlite_conn.cursor()
        
        # Önce mevcut projeleri kontrol et
        cursor.execute("SELECT COUNT(*) FROM projects WHERE is_active = 1")
        total_projects = cursor.fetchone()[0]
        print(f" Toplam aktif proje sayısı: {total_projects}")
        
        # Senkronize edilmemiş projeleri al
        cursor.execute("""
            SELECT p.*, 
                   CASE 
                       WHEN p.supabase_id IS NULL THEN 'not_synced'
                       WHEN p.sync_status IS NULL THEN 'not_synced'
                       ELSE p.sync_status 
                   END as current_sync_status
            FROM projects p
            WHERE p.is_active = 1
            -- AND (
            --     p.sync_status != 'synced' 
            --     OR p.sync_status IS NULL 
            --     OR p.supabase_id IS NULL
            -- )
        """)
        projects = cursor.fetchall()
        
        print(f" Senkronize edilecek proje sayısı: {len(projects)}")
        
        successful_syncs = 0
        failed_syncs = 0
        skipped_syncs = 0
        
        for project in projects:
            print(f"\n Proje senkronize ediliyor: {project['name']} (ID: {project['id']})")
            print(f"   Mevcut durum: {project['current_sync_status']}")
            
            try:
                # Supabase'de projenin var olup olmadığını kontrol et
                if project['supabase_id']:
                    # Bu kontrol Supabase'de yapılmalı, SQLite'da değil
                    print(f"   Proje zaten bir Supabase ID'si var: {project['supabase_id']}")
                    skipped_syncs += 1
                    continue
                
                # Supabase'e proje oluştur
                supabase_project_id = supabase_manager.create_wscad_project(
                    project['name'],
                    project['description'],
                    project['created_by'],
                    project['id']
                )
                
                if supabase_project_id:
                    # SQLite'da senkronizasyon durumunu güncelle
                    cursor.execute("""
                        UPDATE projects 
                        SET supabase_id = ?, 
                            sync_status = 'synced',
                            sync_date = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (supabase_project_id, project['id']))
                    sqlite_conn.commit()
                    
                    print(f"   Proje başarıyla senkronize edildi")
                    print(f"   SQLite ID: {project['id']} -> Supabase ID: {supabase_project_id}")
                    successful_syncs += 1
                else:
                    print(f"   Proje oluşturulamadı")
                    failed_syncs += 1
                    
            except Exception as e:
                print(f"   Proje senkronizasyon hatası: {str(e)}")
                failed_syncs += 1
        
        sqlite_conn.close()
        
        print(f"\n Migration özeti:")
        print(f"   Başarılı: {successful_syncs}")
        print(f"   Başarısız: {failed_syncs}")
        print(f"   Atlanan: {skipped_syncs}")
        print(f"   📝 Toplam: {total_projects}")
        
        # Consider it a success if either new syncs were successful or all items were already synced
        return successful_syncs > 0 or (skipped_syncs > 0 and failed_syncs == 0)
        
    except Exception as e:
        print(f" Proje migration hatası: {str(e)}")
        return False

def migrate_existing_comparisons_to_supabase(sqlite_db, supabase_manager):
    """Mevcut karşılaştırmaları SQLite'dan Supabase'e migrate et"""
    try:
        sqlite_conn = get_sqlite_connection(sqlite_db)
        if not sqlite_conn:
            return False
        
        cursor = sqlite_conn.cursor()
        
        # Önce karşılaştırma tablosunun yapısını kontrol et
        cursor.execute("PRAGMA table_info(wscad_comparisons)")
        columns = [column[1] for column in cursor.fetchall()]
        print(f"📋 Karşılaştırma tablosu sütunları: {', '.join(columns)}")
        
        # Senkronize edilmemiş karşılaştırmaları al
        cursor.execute("""
            SELECT wc.*, p.supabase_id as project_supabase_id
            FROM wscad_comparisons wc
            JOIN projects p ON wc.project_id = p.id
            WHERE (wc.supabase_saved != 1 OR wc.supabase_saved IS NULL) AND p.supabase_id IS NOT NULL
        """)
        comparisons = cursor.fetchall()
        
        print(f"📊 Toplam karşılaştırma sayısı: {len(comparisons)}")
        
        successful_syncs = 0
        failed_syncs = 0
        
        for comp in comparisons:
            print(f"\n🔄 Karşılaştırma senkronize ediliyor: {comp['id']}")
            
            try:
                # Karşılaştırma verilerini parse et
                comparison_data = []
                summary_field = 'comparison_summary'
                
                if summary_field in columns and comp[summary_field]:
                    try:
                        print(f"   📄 Karşılaştırma verisi: {comp[summary_field][:30]}...")
                        summary_data = json.loads(comp[summary_field])
                        comparison_data = summary_data.get('changes', [])
                        print(f"   📊 Değişiklik sayısı: {len(comparison_data)}")
                    except Exception as e:
                        print(f"   ⚠️ Comparison data parse hatası: {str(e)}")
                        continue
                else:
                    print(f"   ⚠️ Karşılaştırma özeti bulunamadı")
                    continue

                # Supabase'e karşılaştırma kaydet
                comparison_id = supabase_manager.save_wscad_comparison_to_project(
                    project_id=comp['project_supabase_id'],
                    comparison_data=comparison_data,
                    file1_name=comp.get('file1_name', 'Unknown'),
                    file2_name=comp.get('file2_name', 'Unknown'),
                    file1_info={
                        'is_emri_no': comp.get('file1_is_emri_no'),
                        'proje_adi': comp.get('file1_proje_adi'),
                        'revizyon_no': comp.get('file1_revizyon_no')
                    },
                    file2_info={
                        'is_emri_no': comp.get('file2_is_emri_no'),
                        'proje_adi': comp.get('file2_proje_adi'),
                        'revizyon_no': comp.get('file2_revizyon_no')
                    },
                    created_by=comp.get('created_by')
                )

                if comparison_id:
                    # SQLite'da senkronizasyon durumunu güncelle
                    cursor.execute("""
                        UPDATE wscad_comparisons 
                        SET supabase_saved = 1,
                            supabase_comparison_id = ?,
                            sync_date = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (comparison_id, comp['id']))
                    sqlite_conn.commit()
                    
                    print(f"✅ Karşılaştırma senkronize edildi: {comp['id']}")
                    successful_syncs += 1
                else:
                    print(f"❌ Karşılaştırma senkronize edilemedi: {comp['id']}")
                    failed_syncs += 1
                    
            except Exception as e:
                print(f"❌ Karşılaştırma {comp['id']} senkronizasyon hatası: {e}")
                failed_syncs += 1
                continue
        
        sqlite_conn.close()
        
        print(f"📊 Migration özeti: {successful_syncs} başarılı, {failed_syncs} başarısız")
        # Consider it a success if either new syncs were successful or there were no items to sync
        return successful_syncs > 0 or (len(comparisons) == 0 and failed_syncs == 0)
        
    except Exception as e:
        print(f"❌ Karşılaştırma migration hatası: {e}")
        return False

if __name__ == "__main__":
    print("🚀 WSCAD Migration Tool")
    print("=" * 50)
    
    # Initialize Supabase manager
    print("\n📡 Supabase bağlantısı kuruluyor...")
    supabase_manager = SupabaseManager()
    
    if not supabase_manager.is_connected():
        print("❌ Supabase bağlantısı kurulamadı!")
        exit(1)
    
    # Fix table structure first
    print("\n🔧 Tablo yapısı kontrol ediliyor...")
    if not supabase_manager.fix_table_structure():
        print("❌ Tablo yapısı düzeltilemedi!")
        exit(1)
    
    # Migrate projects
    print("\n🔄 Projeler migrate ediliyor...")
    if migrate_wscad_projects_to_supabase("wscad_comparison.db", supabase_manager):
        print("\n✅ Proje migration tamamlandı!")
    else:
        print("\n❌ Proje migration başarısız!")
    
    # Migrate comparisons
    print("\n🔄 Karşılaştırmalar migrate ediliyor...")
    if migrate_existing_comparisons_to_supabase("wscad_comparison.db", supabase_manager):
        print("\n✅ Karşılaştırma migration tamamlandı!")
    else:
        print("\n❌ Karşılaştırma migration başarısız!")
    
    print("\n✨ Migration işlemi tamamlandı!")
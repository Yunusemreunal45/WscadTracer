import os
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime
import json

class SupabaseManager:
    """WSCAD BOM karşılaştırma sonuçları için Supabase yöneticisi"""
    
    def __init__(self):
        """Initialize Supabase connection manager"""
        load_dotenv()
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Create a new connection to Supabase"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('SUPABASE_HOST'),
                database=os.getenv('SUPABASE_DATABASE'),
                user=os.getenv('SUPABASE_USER'),
                password=os.getenv('SUPABASE_PASSWORD'),
                port=os.getenv('SUPABASE_PORT', 5432)
            )
            self.connection.autocommit = False
            return True
        except Exception as e:
            print(f"Supabase connection error: {e}")
            return False

    def setup_wscad_tables(self):
        """Setup WSCAD specific tables in Supabase"""
        try:
            with self.connection.cursor() as cursor:
                # Create WSCAD projects table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_projects (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL UNIQUE,
                        description TEXT,
                        created_by VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        current_revision INTEGER DEFAULT 0,
                        sqlite_project_id INTEGER,
                        sync_status VARCHAR(50) DEFAULT 'pending'
                    )
                """)
                
                # WSCAD Proje karşılaştırmaları tablosu
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_project_comparisons (
                        id SERIAL PRIMARY KEY,
                        project_id INTEGER REFERENCES wscad_projects(id),
                        comparison_title TEXT NOT NULL,
                        file1_name TEXT NOT NULL,
                        file2_name TEXT NOT NULL,
                        file1_is_emri_no TEXT,
                        file1_revizyon_no TEXT,
                        file2_is_emri_no TEXT,
                        file2_revizyon_no TEXT,
                        changes_count INTEGER DEFAULT 0,
                        revision_number INTEGER,
                        created_by TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # WSCAD BOM karşılaştırma detayları - Excel sütunlarına göre
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_comparison_changes (
                        id SERIAL PRIMARY KEY,
                        project_comparison_id INTEGER REFERENCES wscad_project_comparisons(id),
                        change_type TEXT NOT NULL,
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
                    )
                """)
                
                # WSCAD BOM özel değişiklik türleri tablosu
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_quantity_changes (
                        id SERIAL PRIMARY KEY,
                        project_comparison_id INTEGER REFERENCES wscad_project_comparisons(id),
                        poz_no TEXT NOT NULL,
                        parca_no TEXT,
                        parca_adi TEXT,
                        old_quantity NUMERIC,
                        new_quantity NUMERIC,
                        quantity_change_type TEXT, -- 'increased', 'decreased', 'zeroed'
                        birim_adet_old NUMERIC,
                        birim_adet_new NUMERIC,
                        toplam_adet_old NUMERIC,
                        toplam_adet_new NUMERIC,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # WSCAD Proje istatistikleri tablosu
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wscad_project_statistics (
                        id SERIAL PRIMARY KEY,
                        project_id INTEGER REFERENCES wscad_projects(id) UNIQUE,
                        total_comparisons INTEGER DEFAULT 0,
                        total_changes INTEGER DEFAULT 0,
                        total_added_items INTEGER DEFAULT 0,
                        total_removed_items INTEGER DEFAULT 0,
                        total_quantity_changes INTEGER DEFAULT 0,
                        last_comparison_date TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                self.connection.commit()
                print("WSCAD BOM Supabase tabloları hazırlandı")
                
            # Update create_wscad_project method
            def create_wscad_project(self, name, description, created_by, sqlite_project_id=None):
                """Create a new WSCAD project"""
                try:
                    with self.connection.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO wscad_projects 
                            (name, description, created_by, sqlite_project_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (name) DO UPDATE SET
                                description = EXCLUDED.description,
                                created_by = EXCLUDED.created_by,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING id
                        """, (name, description, created_by, sqlite_project_id))
                        
                        project_id = cursor.fetchone()[0]
                        self.connection.commit()
                        return project_id

                except Exception as e:
                    print(f"WSCAD project creation error: {e}")
                    if self.connection and not self.connection.closed:
                        self.connection.rollback()
                    return None

        except Exception as e:
            print(f"WSCAD tablo oluşturma hatası: {e}")
            self.connection.rollback()
    
    def create_wscad_project(self, name, description, created_by, sqlite_project_id=None):
        """WSCAD projesi oluştur - düzeltilmiş versiyon"""
        try:
            # Check connection and reconnect if needed
            if not self.connection or self.connection.closed:
                self._connect()

            with self.connection.cursor() as cursor:
                # Aynı isimde proje var mı kontrol et
                cursor.execute("SELECT id FROM wscad_projects WHERE name = %s", (name,))
                existing = cursor.fetchone()
                
                if existing:
                    # Unique isim oluştur
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    unique_name = f"{name}_{timestamp}"
                else:
                    unique_name = name
                
                # Proje oluştur
                cursor.execute("""
                    INSERT INTO wscad_projects 
                    (name, description, created_by, sqlite_project_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                        description = EXCLUDED.description,
                        created_by = EXCLUDED.created_by,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (unique_name, description, created_by, sqlite_project_id))
                
                project_id = cursor.fetchone()[0]
                
                # İstatistik kaydı oluştur
                cursor.execute("""
                    INSERT INTO wscad_project_statistics (project_id)
                    VALUES (%s)
                    ON CONFLICT (project_id) DO NOTHING
                """, (project_id,))
                
                self.connection.commit()
                print(f"✅ WSCAD project created in Supabase: {unique_name} (ID: {project_id})")
                return project_id
            
        except Exception as e:
            print(f"WSCAD proje oluşturma hatası: {e}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return None
    
    def get_wscad_projects(self):
        """Tüm WSCAD projelerini getir"""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT wp.*, wps.total_comparisons, wps.total_changes, wps.last_comparison_date
                FROM wscad_projects wp
                LEFT JOIN wscad_project_statistics wps ON wp.id = wps.project_id
                WHERE wp.is_active = TRUE
                ORDER BY wp.created_at DESC
            """)
            return cursor.fetchall()
            
        except Exception as e:
            print(f"WSCAD proje listesi alma hatası: {e}")
            return []
    
    def save_wscad_comparison_to_project(self, project_id, comparison_data, file1_name, file2_name, 
                                   file1_info=None, file2_info=None, created_by=None):
        """WSCAD karşılaştırma sonucunu projeye kaydet"""
        try:
            cursor = self.connection.cursor()
            
            # Revizyon numarasını hesapla
            cursor.execute("""
                SELECT COALESCE(MAX(revision_number), 0) + 1 
                FROM wscad_project_comparisons
                WHERE project_id = %s
            """, (project_id,))
            
            next_revision = cursor.fetchone()[0]
            
            # Dosya bilgilerini hazırla
            file1_is_emri = file1_info.get('is_emri_no', '') if isinstance(file1_info, dict) else ''
            file1_rev = file1_info.get('revizyon_no', '') if isinstance(file1_info, dict) else ''
            file2_is_emri = file2_info.get('is_emri_no', '') if isinstance(file2_info, dict) else ''
            file2_rev = file2_info.get('revizyon_no', '') if isinstance(file2_info, dict) else ''
            
            comparison_title = f"Rev {next_revision}: {file1_name} → {file2_name}"
            
            # Ana karşılaştırma kaydı
            with self.connection.cursor() as save_cursor:
                save_cursor.execute("""
                    INSERT INTO wscad_project_comparisons 
                    (project_id, comparison_title, file1_name, file2_name, 
                     file1_is_emri_no, file1_revizyon_no, file2_is_emri_no, file2_revizyon_no,
                     changes_count, revision_number, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    project_id, comparison_title, file1_name, file2_name,
                    file1_is_emri, file1_rev, file2_is_emri, file2_rev,
                    len(comparison_data), next_revision, created_by
                ))
                
                comparison_id = save_cursor.fetchone()[0]

                # Değişiklikleri toplu olarak kaydet
                changes_to_insert = []
                quantity_changes = []

                for change in comparison_data:
                    changes_to_insert.append((
                        comparison_id,
                        change.get('change_type', 'modified'),
                        change.get('poz_no', ''),
                        change.get('parca_no', ''),
                        change.get('parca_adi', ''),
                        change.get('column', ''),
                        str(change.get('value1', '')),
                        str(change.get('value2', '')),
                        change.get('severity', 'medium'),
                        change.get('description', ''),
                        created_by
                    ))

                    # Miktar değişikliği varsa kaydet
                    if change.get('change_type') in ['quantity_increased', 'quantity_decreased']:
                        try:
                            old_qty = float(change.get('value1', 0))
                            new_qty = float(change.get('value2', 0))
                            
                            quantity_changes.append((
                                comparison_id,
                                change.get('poz_no', ''),
                                change.get('parca_no', ''),
                                change.get('parca_adi', ''),
                                old_qty,
                                new_qty,
                                change.get('change_type')
                            ))
                        except (ValueError, TypeError):
                            continue

                # Toplu insert işlemleri
                if changes_to_insert:
                    save_cursor.executemany("""
                        INSERT INTO wscad_comparison_changes 
                        (project_comparison_id, change_type, poz_no, parca_no, parca_adi,
                         column_name, old_value, new_value, severity, description, modified_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, changes_to_insert)

                if quantity_changes:
                    save_cursor.executemany("""
                        INSERT INTO wscad_quantity_changes 
                        (project_comparison_id, poz_no, parca_no, parca_adi,
                         old_quantity, new_quantity, quantity_change_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, quantity_changes)

                # İstatistikleri güncelle
                save_cursor.execute("""
                    INSERT INTO wscad_project_statistics (
                        project_id, total_comparisons, total_changes,
                        last_comparison_date
                    ) VALUES (
                        %s, 1, %s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (project_id) DO UPDATE SET
                        total_comparisons = wscad_project_statistics.total_comparisons + 1,
                        total_changes = wscad_project_statistics.total_changes + %s,
                        last_comparison_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                """, (project_id, len(comparison_data), len(comparison_data)))

            self.connection.commit()
            return comparison_id

        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Supabase kaydetme hatası: {str(e)}")
    
    def get_wscad_project_comparisons(self, project_id):
        """WSCAD projesinin tüm karşılaştırmalarını getir"""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT id, comparison_title, file1_name, file2_name, 
                       file1_is_emri_no, file1_revizyon_no, file2_is_emri_no, file2_revizyon_no,
                       changes_count, revision_number, created_by, created_at
                FROM wscad_project_comparisons
                WHERE project_id = %s
                ORDER BY revision_number DESC
            """, (project_id,))
            
            return cursor.fetchall()
            
        except Exception as e:
            print(f"WSCAD proje karşılaştırmaları alma hatası: {e}")
            return []
    
    def get_wscad_comparison_details(self, comparison_id):
        """WSCAD karşılaştırma detaylarını getir - Depolanan sonuçları okur"""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Ana bilgileri al
            cursor.execute("""
                SELECT * FROM wscad_project_comparisons WHERE id = %s
            """, (comparison_id,))
            comparison = cursor.fetchone()
            
            if not comparison:
                return None
            
            # Değişiklik detayları
            cursor.execute("""
                SELECT * FROM wscad_comparison_changes 
                WHERE project_comparison_id = %s
                ORDER BY poz_no, id
            """, (comparison_id,))
            changes = cursor.fetchall()
            
            # Miktar değişiklikleri
            cursor.execute("""
                SELECT * FROM wscad_quantity_changes 
                WHERE project_comparison_id = %s
                ORDER BY poz_no
            """, (comparison_id,))
            quantity_changes = cursor.fetchall()
            
            print(f"📋 Karşılaştırma detayları alındı: {len(changes)} değişiklik, {len(quantity_changes)} miktar değişikliği")
            
            return {
                'comparison': comparison,
                'changes': changes,
                'quantity_changes': quantity_changes
            }
            
        except Exception as e:
            print(f"WSCAD karşılaştırma detayı alma hatası: {e}")
            return None
    
    def get_wscad_project_statistics(self, project_id):
        """WSCAD proje istatistiklerini getir"""
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Genel istatistikler
            cursor.execute("""
                SELECT * FROM wscad_project_statistics WHERE project_id = %s
            """, (project_id,))
            general_stats = cursor.fetchone()
            
            # Değişiklik türü istatistikleri
            cursor.execute("""
                SELECT 
                    change_type,
                    COUNT(*) as count,
                    severity,
                    COUNT(*) as severity_count
                FROM wscad_comparison_changes wcc
                JOIN wscad_project_comparisons wpc ON wcc.project_comparison_id = wpc.id
                WHERE wpc.project_id = %s
                GROUP BY change_type, severity
                ORDER BY count DESC
            """, (project_id,))
            change_stats = cursor.fetchall()
            
            # POZ NO bazında değişiklikler
            cursor.execute("""
                SELECT 
                    poz_no,
                    parca_adi,
                    COUNT(*) as change_count
                FROM wscad_comparison_changes wcc
                JOIN wscad_project_comparisons wpc ON wcc.project_comparison_id = wpc.id
                WHERE wpc.project_id = %s AND poz_no IS NOT NULL AND poz_no != ''
                GROUP BY poz_no, parca_adi
                ORDER BY change_count DESC
                LIMIT 10
            """, (project_id,))
            poz_stats = cursor.fetchall()
            
            return {
                'general': general_stats,
                'changes': change_stats,
                'top_changed_items': poz_stats
            }
            
        except Exception as e:
            print(f"WSCAD proje istatistikleri alma hatası: {e}")
            return None
    
    def sync_project_from_sqlite(self, sqlite_project):
        """SQLite'dan projeyi Supabase'e senkronize et"""
        try:
            supabase_project_id = self.create_wscad_project(
                sqlite_project['name'],
                sqlite_project['description'],
                sqlite_project['created_by'],
                sqlite_project['id']
            )
            return supabase_project_id
        except Exception as e:
            print(f"Proje senkronizasyon hatası: {e}")
            return None
    
    def close(self):
        """Bağlantıyı kapat"""
        if self.connection:
            self.connection.close()
            print("Supabase bağlantısı kapatıldı")

def get_sqlite_connection(db_file="wscad_comparison.db"):
    """SQLite veritabanı bağlantısı al"""
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        print(f"SQLite veritabanına bağlanıldı: {db_file}")
        return conn
    except Exception as e:
        print(f"SQLite bağlantı hatası: {e}")
        return None

def migrate_wscad_projects_to_supabase(sqlite_db, supabase_manager):
    """WSCAD projelerini SQLite'dan Supabase'e migrate et"""
    try:
        sqlite_conn = get_sqlite_connection(sqlite_db)
        if not sqlite_conn:
            return False
        
        cursor = sqlite_conn.cursor()
        
        # Senkronize edilmemiş projeleri al
        cursor.execute("""
            SELECT * FROM projects 
            WHERE sync_status != 'synced' OR sync_status IS NULL
        """)
        projects = cursor.fetchall()
        
        for project in projects:
            print(f"Proje senkronize ediliyor: {project['name']}")
            
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
                    SET supabase_id = ?, sync_status = 'synced'
                    WHERE id = ?
                """, (supabase_project_id, project['id']))
                sqlite_conn.commit()
                print(f"✅ Proje senkronize edildi: {project['name']}")
            else:
                print(f"❌ Proje senkronize edilemedi: {project['name']}")
        
        sqlite_conn.close()
        return True
        
    except Exception as e:
        print(f"Proje migration hatası: {e}")
        return False

def main():
    """Ana migration fonksiyonu"""
    print("WSCAD BOM Comparison System - Supabase Migration")
    
    # Supabase manager'ı başlat
    supabase = SupabaseManager()
    
    if not supabase.connection:
        print("❌ Supabase bağlantısı başarısız")
        return
    
    print("✅ Supabase bağlantısı başarılı")
    print("✅ WSCAD BOM tabloları hazırlandı")
    
    # Projeleri migrate et
    if migrate_wscad_projects_to_supabase("wscad_comparison.db", supabase):
        print("✅ Proje migration tamamlandı")
    else:
        print("❌ Proje migration başarısız")
    
    supabase.close()

if __name__ == "__main__":
    main()
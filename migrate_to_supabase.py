import os
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime
import json
import hashlib

class SupabaseManager:
    """WSCAD BOM karşılaştırma sonuçları için geliştirilmiş Supabase yöneticisi"""
    
    def __init__(self):
        """Initialize Supabase connection manager"""
        load_dotenv()
        self.connection = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3
        self._connect()
    
    def _connect(self):
        """Create a new connection to Supabase with retry logic"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('SUPABASE_HOST'),
                database=os.getenv('SUPABASE_DATABASE'),
                user=os.getenv('SUPABASE_USER'),
                password=os.getenv('SUPABASE_PASSWORD'),
                port=os.getenv('SUPABASE_PORT', 5432),
                connect_timeout=10
            )
            self.connection.autocommit = False
            self.reconnect_attempts = 0
            print("✅ Supabase connection established")
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
                print("✅ Supabase bağlantısı kapatıldı")
        except Exception as e:
            print(f"⚠️ Supabase connection close error: {e}")

    def reconnect(self):
        """Reconnect to Supabase with retry logic"""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            print(f"🔄 Attempting to reconnect to Supabase ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
            return self._connect()
        else:
            print(f"❌ Max reconnection attempts reached")
            return False

    def is_connected(self):
        """Check if connection is alive"""
        try:
            if not self.connection or self.connection.closed:
                return False
            
            # Test connection with a simple query
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except:
            return False

    def setup_wscad_tables(self):
        """Setup WSCAD specific tables in Supabase"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False

            with self.connection.cursor() as cursor:
                # First drop all existing tables
                cursor.execute("""
                    DROP TABLE IF EXISTS wscad_quantity_changes CASCADE;
                    DROP TABLE IF EXISTS wscad_comparison_changes CASCADE;
                    DROP TABLE IF EXISTS wscad_project_comparisons CASCADE;
                    DROP TABLE IF EXISTS wscad_project_statistics CASCADE;
                    DROP TABLE IF EXISTS wscad_projects CASCADE;
                """)

                # Create tables in single transaction
                cursor.execute("""
                    CREATE TABLE wscad_projects (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT,
                        created_by TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        sqlite_project_id INTEGER,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        project_type VARCHAR(50) DEFAULT 'wscad',
                        CONSTRAINT wscad_projects_name_unique UNIQUE (name, created_by)
                    );

                    CREATE INDEX idx_wscad_projects_name ON wscad_projects(name);
                    CREATE INDEX idx_wscad_projects_created_by ON wscad_projects(created_by);
                """)

                self.connection.commit()
                print("✅ WSCAD tables created successfully")
                return True

        except Exception as e:
            print(f"❌ Table setup error: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return False

    def create_wscad_project(self, name, description, created_by, sqlite_project_id=None):
        """Create a new WSCAD project"""
        try:
            if not self.is_connected():
                self.reconnect()

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO wscad_projects 
                    (name, description, created_by, sqlite_project_id, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT ON CONSTRAINT wscad_projects_name_unique 
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (name, description, created_by, sqlite_project_id))
                
                project_id = cursor.fetchone()[0]
                self.connection.commit()
                print(f"✅ Project created/updated successfully: {name} (ID: {project_id})")
                return project_id

        except Exception as e:
            print(f"❌ Project creation error: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return None
    
    def get_wscad_projects(self, created_by=None):
        """Tüm WSCAD projelerini getir - filtreleme desteği ile"""
        try:
            if not self.is_connected() and not self.reconnect():
                return []

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
                FROM wscad_projects wp
                LEFT JOIN wscad_project_statistics wps ON wp.id = wps.project_id
                WHERE wp.is_active = TRUE
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
            return []
    
    def save_wscad_comparison_to_project(self, project_id, comparison_data, file1_name, file2_name, 
                                       file1_info=None, file2_info=None, created_by=None):
        """WSCAD karşılaştırma sonucunu projeye kaydet - optimize edilmiş"""
        try:
            if not self.is_connected() and not self.reconnect():
                return None

            cursor = self.connection.cursor()
            
            # Revizyon numarasını hesapla
            cursor.execute("""
                SELECT COALESCE(MAX(revision_number), 0) + 1 
                FROM wscad_project_comparisons
                WHERE project_id = %s
            """, (project_id,))
            
            next_revision = cursor.fetchone()[0]
            
            # Dosya bilgilerini güvenli şekilde hazırla
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
            comparison_title = f"Rev {next_revision}: {os.path.basename(file1_name)} → {os.path.basename(file2_name)}"
            
            # Karşılaştırma hash'i oluştur
            comparison_hash = hashlib.md5(f"{file1_name}{file2_name}{len(comparison_data)}{datetime.now().isoformat()}".encode()).hexdigest()

            # Karşılaştırma özetini oluştur
            summary_stats = self._generate_comparison_summary(comparison_data)
            comparison_summary = json.dumps(summary_stats, ensure_ascii=False)

            # Ana karşılaştırma kaydı
            cursor.execute("""
                INSERT INTO wscad_project_comparisons 
                (project_id, comparison_title, revision_number, file1_name, file2_name, 
                 file1_is_emri_no, file1_proje_adi, file1_revizyon_no, 
                 file2_is_emri_no, file2_proje_adi, file2_revizyon_no,
                 changes_count, created_by, comparison_hash, comparison_summary,
                 file1_info, file2_info)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                project_id, comparison_title, next_revision, file1_name, file2_name,
                file1_is_emri, file1_proje, file1_rev, 
                file2_is_emri, file2_proje, file2_rev,
                len(comparison_data), created_by, comparison_hash, comparison_summary,
                json.dumps(file1_info) if file1_info else None,
                json.dumps(file2_info) if file2_info else None
            ))
            
            comparison_id = cursor.fetchone()[0]

            # Değişiklikleri ve miktar değişikliklerini kaydet
            if comparison_data:
                changes_to_insert = []
                quantity_changes = []

                for change in comparison_data:
                    # Değişiklik şiddetini belirle
                    severity = self._determine_change_severity(change)
                    change_category = self._determine_change_category(change)
                    impact_level = self._determine_impact_level(change)
                    
                    changes_to_insert.append((
                        comparison_id,
                        change.get('change_type', 'modified'),
                        change.get('poz_no', '')[:50],
                        change.get('parca_no', '')[:100],
                        change.get('parca_adi', '')[:200],
                        change.get('column', '')[:50],
                        str(change.get('value1', ''))[:500],
                        str(change.get('value2', ''))[:500],
                        severity,
                        change.get('description', '')[:1000],
                        created_by,
                        change_category,
                        impact_level
                    ))

                    # Miktar değişikliği varsa kaydet
                    if self._is_quantity_change(change):
                        qty_change = self._prepare_quantity_change(change, comparison_id)
                        if qty_change:
                            quantity_changes.append(qty_change)

                # Toplu insert işlemleri
                if changes_to_insert:
                    psycopg2.extras.execute_batch(cursor, """
                        INSERT INTO wscad_comparison_changes 
                        (project_comparison_id, change_type, poz_no, parca_no, parca_adi,
                         column_name, old_value, new_value, severity, description, modified_by,
                         change_category, impact_level)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, changes_to_insert, page_size=1000)

                if quantity_changes:
                    psycopg2.extras.execute_batch(cursor, """
                        INSERT INTO wscad_quantity_changes 
                        (project_comparison_id, poz_no, parca_no, parca_adi,
                         old_quantity, new_quantity, quantity_change_type, percentage_change,
                         absolute_change, unit_type, impact_assessment)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, quantity_changes, page_size=500)

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

        except Exception as e:
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            print(f"❌ Supabase kaydetme hatası: {str(e)}")
            return None
    
    def _generate_comparison_summary(self, comparison_data):
        """Karşılaştırma özeti oluştur"""
        summary = {
            'total_changes': len(comparison_data),
            'by_type': {},
            'by_severity': {},
            'by_category': {},
            'quantity_changes': 0,
            'structural_changes': 0,
            'critical_poz_numbers': []
        }
        
        for change in comparison_data:
            change_type = change.get('change_type', 'unknown')
            severity = self._determine_change_severity(change)
            category = self._determine_change_category(change)
            
            # Type statistics
            summary['by_type'][change_type] = summary['by_type'].get(change_type, 0) + 1
            
            # Severity statistics
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # Category statistics
            summary['by_category'][category] = summary['by_category'].get(category, 0) + 1
            
            # Quantity changes
            if self._is_quantity_change(change):
                summary['quantity_changes'] += 1
            
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
    
    def _determine_change_category(self, change):
        """Değişiklik kategorisini belirle"""
        change_type = change.get('change_type', '')
        column = change.get('column', '').lower()
        
        if 'quantity' in change_type or any(qty_col in column for qty_col in ['adet', 'miktar', 'quantity']):
            return 'quantity'
        elif change_type in ['added', 'removed']:
            return 'structural'
        elif any(part_col in column for part_col in ['parca', 'part', 'malzeme', 'material']):
            return 'component'
        elif any(desc_col in column for desc_col in ['aciklama', 'description', 'not', 'note']):
            return 'description'
        else:
            return 'general'
    
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
    
    def _is_quantity_change(self, change):
        """Miktar değişikliği olup olmadığını kontrol et"""
        change_type = change.get('change_type', '')
        column = change.get('column', '').lower()
        
        return ('quantity' in change_type or 
                any(qty_col in column for qty_col in ['adet', 'miktar', 'quantity', 'toplam']))
    
    def _prepare_quantity_change(self, change, comparison_id):
        """Miktar değişikliği için veri hazırla"""
        try:
            old_qty = self._safe_float(change.get('value1', 0))
            new_qty = self._safe_float(change.get('value2', 0))
            
            if old_qty is None or new_qty is None:
                return None
            
            # Absolute change
            absolute_change = new_qty - old_qty
            
            # Percentage change
            percentage_change = 0
            if old_qty != 0:
                percentage_change = ((new_qty - old_qty) / old_qty) * 100
            elif new_qty != 0:
                percentage_change = 100  # New item
            
            # Unit type detection
            column = change.get('column', '').lower()
            unit_type = 'piece'  # default
            if 'kg' in column or 'ağırlık' in column:
                unit_type = 'weight'
            elif 'metre' in column or 'boy' in column:
                unit_type = 'length'
            
            # Impact assessment
            impact_assessment = self._assess_quantity_impact(old_qty, new_qty, percentage_change)
            
            return (
                comparison_id,
                change.get('poz_no', '')[:50],
                change.get('parca_no', '')[:100],
                change.get('parca_adi', '')[:200],
                old_qty,
                new_qty,
                change.get('change_type', 'modified'),
                round(percentage_change, 2),
                round(absolute_change, 3),
                unit_type,
                impact_assessment
            )
        except:
            return None
    
    def _assess_quantity_impact(self, old_qty, new_qty, percentage_change):
        """Miktar değişikliğinin etkisini değerlendir"""
        if new_qty == 0:
            return "Critical: Item quantity set to zero"
        elif old_qty == 0:
            return "New: Item added to BOM"
        elif abs(percentage_change) > 50:
            return f"High Impact: {abs(percentage_change):.1f}% change"
        elif abs(percentage_change) > 20:
            return f"Medium Impact: {abs(percentage_change):.1f}% change"
        else:
            return f"Low Impact: {abs(percentage_change):.1f}% change"
    
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
            quantity_changes = sum(1 for change in comparison_data 
                                 if self._is_quantity_change(change))
            
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
            cursor.execute("""
                INSERT INTO wscad_project_statistics (
                    project_id, total_comparisons, total_changes,
                    total_critical_changes, total_added_items, total_removed_items,
                    total_quantity_changes, last_comparison_date, most_active_contributor,
                    trend_analysis, performance_metrics
                ) VALUES (
                    %s, 1, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s
                )
                ON CONFLICT (project_id) DO UPDATE SET
                    total_comparisons = wscad_project_statistics.total_comparisons + 1,
                    total_changes = wscad_project_statistics.total_changes + %s,
                    total_critical_changes = wscad_project_statistics.total_critical_changes + %s,
                    total_added_items = wscad_project_statistics.total_added_items + %s,
                    total_removed_items = wscad_project_statistics.total_removed_items + %s,
                    total_quantity_changes = wscad_project_statistics.total_quantity_changes + %s,
                    last_comparison_date = CURRENT_TIMESTAMP,
                    most_active_contributor = %s,
                    average_changes_per_comparison = (wscad_project_statistics.total_changes + %s) / 
                                                   (wscad_project_statistics.total_comparisons + 1),
                    trend_analysis = %s,
                    performance_metrics = %s,
                    updated_at = CURRENT_TIMESTAMP
            """, (project_id, changes_count, critical_changes, added_items, removed_items,
                  quantity_changes, created_by, json.dumps(trend_data), json.dumps(performance_metrics),
                  changes_count, critical_changes, added_items, removed_items, quantity_changes,
                  created_by, changes_count, json.dumps(trend_data), json.dumps(performance_metrics)))
            
        except Exception as e:
            print(f"⚠️ Statistics update error: {e}")
    
    def get_wscad_project_comparisons(self, project_id, limit=50):
        """WSCAD projesinin tüm karşılaştırmalarını getir - sayfalama ile"""
        try:
            if not self.is_connected() and not self.reconnect():
                return []

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT id, comparison_title, file1_name, file2_name, 
                       file1_is_emri_no, file1_revizyon_no, file2_is_emri_no, file2_revizyon_no,
                       file1_proje_adi, file2_proje_adi,
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
            return []
    
    def get_wscad_comparison_details(self, comparison_id):
        """WSCAD karşılaştırma detaylarını getir - optimize edilmiş"""
        try:
            if not self.is_connected() and not self.reconnect():
                return None

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Ana bilgileri al
            cursor.execute("""
                SELECT * FROM wscad_project_comparisons WHERE id = %s
            """, (comparison_id,))
            comparison = cursor.fetchone()
            
            if not comparison:
                return None
            
            # Değişiklik detayları - sayfalama ile
            cursor.execute("""
                SELECT * FROM wscad_comparison_changes 
                WHERE project_comparison_id = %s
                ORDER BY 
                    CASE severity 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        WHEN 'low' THEN 3
                        ELSE 4 
                    END,
                    poz_no, id
                LIMIT 1000
            """, (comparison_id,))
            changes = cursor.fetchall()
            
            # Miktar değişiklikleri
            cursor.execute("""
                SELECT * FROM wscad_quantity_changes 
                WHERE project_comparison_id = %s
                ORDER BY ABS(percentage_change) DESC, poz_no
                LIMIT 500
            """, (comparison_id,))
            quantity_changes = cursor.fetchall()
            
            # Özet istatistikler
            cursor.execute("""
                SELECT 
                    change_type,
                    severity,
                    change_category,
                    COUNT(*) as count
                FROM wscad_comparison_changes 
                WHERE project_comparison_id = %s
                GROUP BY change_type, severity, change_category
                ORDER BY count DESC
            """, (comparison_id,))
            change_summary = cursor.fetchall()
            
            print(f"📋 Karşılaştırma detayları alındı: {len(changes)} değişiklik, {len(quantity_changes)} miktar değişikliği")
            
            return {
                'comparison': comparison,
                'changes': changes,
                'quantity_changes': quantity_changes,
                'summary': change_summary
            }
            
        except Exception as e:
            print(f"❌ WSCAD karşılaştırma detayı alma hatası: {e}")
            return None
    
    def get_wscad_project_statistics(self, project_id):
        """WSCAD proje istatistiklerini getir - geliştirilmiş"""
        try:
            if not self.is_connected() and not self.reconnect():
                return None

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
                    change_category,
                    COUNT(*) as count
                FROM wscad_comparison_changes wcc
                JOIN wscad_project_comparisons wpc ON wcc.project_comparison_id = wpc.id
                WHERE wpc.project_id = %s
                GROUP BY change_type, severity, change_category
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
                    MAX(wcc.created_at) as last_change_date
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
                    comparison_title,
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
            
            return {
                'general': general_stats,
                'changes': change_stats,
                'top_changed_items': poz_stats,
                'trends': trend_stats,
                'revisions': revision_stats
            }
            
        except Exception as e:
            print(f"❌ WSCAD proje istatistikleri alma hatası: {e}")
            return None
    
    def get_project_revision_history(self, project_id, limit=20):
        """Proje revizyon geçmişini detaylı olarak getir"""
        try:
            if not self.is_connected() and not self.reconnect():
                return []

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT 
                    wpc.*,
                    COUNT(wcc.id) as detailed_changes_count,
                    COUNT(CASE WHEN wcc.severity = 'high' THEN 1 END) as critical_changes,
                    COUNT(CASE WHEN wcc.severity = 'medium' THEN 1 END) as medium_changes,
                    COUNT(CASE WHEN wcc.severity = 'low' THEN 1 END) as low_changes,
                    COUNT(wqc.id) as quantity_changes_count,
                    COALESCE(AVG(wqc.percentage_change), 0) as avg_quantity_change_percentage
                FROM wscad_project_comparisons wpc
                LEFT JOIN wscad_comparison_changes wcc ON wpc.id = wcc.project_comparison_id
                LEFT JOIN wscad_quantity_changes wqc ON wpc.id = wqc.project_comparison_id
                WHERE wpc.project_id = %s AND wpc.status = 'active'
                GROUP BY wpc.id
                ORDER BY wpc.revision_number DESC
                LIMIT %s
            """, (project_id, limit))
            
            return cursor.fetchall()
            
        except Exception as e:
            print(f"❌ Revizyon geçmişi alma hatası: {e}")
            return []
    
    def get_recent_comparisons(self, limit=20, created_by=None):
        """Son karşılaştırmaları getir"""
        try:
            if not self.is_connected() and not self.reconnect():
                return []

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            query = """
                SELECT 
                    wpc.id,
                    wp.name as project_name,
                    wpc.comparison_title,
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
            print(f"❌ Recent comparisons hatası: {e}")
            return []
    
    def search_projects(self, search_term, created_by=None):
        """Proje arama"""
        try:
            if not self.is_connected() and not self.reconnect():
                return []

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
            print(f"❌ Project search hatası: {e}")
            return []
    
    def delete_project(self, project_id, created_by=None):
        """Projeyi soft delete et"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False

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
            print(f"❌ Project deletion error: {e}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return False
    
    def archive_revision(self, comparison_id, created_by=None):
        """Revizyonu arşivle"""
        try:
            if not self.is_connected() and not self.reconnect():
                return False

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
            print(f"❌ Revision archive error: {e}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return False
    
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
            print(f"❌ Proje senkronizasyon hatası: {e}")
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
            """, (project_id,))
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
        
        # Senkronize edilmemiş projeleri al
        cursor.execute("""
            SELECT * FROM projects 
            WHERE (sync_status != 'synced' OR sync_status IS NULL)
            AND is_active = 1
        """)
        projects = cursor.fetchall()
        
        successful_syncs = 0
        failed_syncs = 0
        
        for project in projects:
            print(f"🔄 Proje senkronize ediliyor: {project['name']}")
            
            try:
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
                    successful_syncs += 1
                else:
                    print(f"❌ Proje senkronize edilemedi: {project['name']}")
                    failed_syncs += 1
                    
            except Exception as e:
                print(f"❌ Proje {project['name']} senkronizasyon hatası: {e}")
                failed_syncs += 1
        
        sqlite_conn.close()
        
        print(f"📊 Migration özeti: {successful_syncs} başarılı, {failed_syncs} başarısız")
        return successful_syncs > 0
        
    except Exception as e:
        print(f"❌ Proje migration hatası: {e}")
        return False

def migrate_existing_comparisons_to_supabase(sqlite_db, supabase_manager):
    """Mevcut karşılaştırmaları SQLite'dan Supabase'e migrate et"""
    try:
        sqlite_conn = get_sqlite_connection(sqlite_db)
        if not sqlite_conn:
            return False
        
        cursor = sqlite_conn.cursor()
        
        # Senkronize edilmemiş karşılaştırmaları al
        cursor.execute("""
            SELECT wc.*, p.supabase_id as project_supabase_id
            FROM wscad_comparisons wc
            JOIN projects p ON wc.file_id = p.id
            WHERE wc.supabase_saved != 1 AND p.supabase_id IS NOT NULL
        """)
        comparisons = cursor.fetchall()
        
        successful_syncs = 0
        failed_syncs = 0
        
        for comp in comparisons:
            print(f"🔄 Karşılaştırma senkronize ediliyor: {comp['id']}")
            
            try:
                # Karşılaştırma verilerini parse et
                comparison_data = []
                if comp['comparison_summary']:
                    try:
                        summary_data = json.loads(comp['comparison_summary'])
                        comparison_data = summary_data.get('changes', [])
                    except:
                        print(f"⚠️ Comparison data parse hatası: {comp['id']}")
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
        return successful_syncs > 0
        
    except Exception as e:
        print(f"❌ Karşılaştırma migration hatası: {e}")
        return False
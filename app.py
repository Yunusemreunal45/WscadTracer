import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import glob
import json

# Import custom modules - dosya isimleri değişmedi
from auth import authenticate
from database import Database
from excel_processor import ExcelProcessor
from erp_exporter import ERPExporter
from migrate_to_supabase import SupabaseManager  # SupabaseManager sınıfını import et
from utils import get_file_info, log_activity

# Page configuration
st.set_page_config(
    page_title="WSCAD BOM Comparison System",
    page_icon="📊",
    layout="wide"
)

# Initialize database using SQLite (yerel kullanıcı verileri için)
@st.cache_resource
def get_database():
    """Singleton veri tabanı oluştur (thread güvenliği için)"""
    db = Database()
    setup_success = db.setup_database()
    return db, setup_success

# Initialize Supabase manager (karşılaştırma sonuçları için)
@st.cache_resource
def get_supabase_manager():
    """Initialize Supabase connection with reconnection support"""
    try:
        supabase = SupabaseManager()
        if not supabase.connection or supabase.connection.closed:
            supabase.reconnect()
        return supabase
    except Exception as e:
        st.error(f"Supabase connection error: {e}")
        return None

db, setup_success = get_database()
supabase = get_supabase_manager()

# Initialize processors - mevcut sınıf isimleri korundu
excel_processor = ExcelProcessor()
erp_exporter = ERPExporter()

def scan_xlsx_files(directory):
    """Belirtilen dizindeki .xlsx dosyalarını tarar ve WSCAD kontrolü yapar"""
    try:
        if not os.path.exists(directory):
            return []
        
        xlsx_pattern = os.path.join(directory, "**", "*.xlsx")
        xlsx_files = glob.glob(xlsx_pattern, recursive=True)
        
        unique_files = set()
        for filepath in xlsx_files:
            normalized_path = os.path.normpath(filepath)
            unique_files.add(normalized_path)
        
        wscad_files = []
        for filepath in unique_files:
            try:
                filename = os.path.basename(filepath)
                if filename.startswith('~$'):
                    continue
                
                # WSCAD dosyası kontrolü
                if excel_processor.is_wscad_excel(filepath):
                    stat = os.stat(filepath)
                    
                    # Proje bilgilerini çıkar
                    try:
                        file_info = excel_processor.process_file(filepath)
                        project_info = file_info.get('project_info', {})
                    except:
                        project_info = {}
                    
                    wscad_files.append({
                        'filepath': filepath,
                        'filename': filename,
                        'size_kb': round(stat.st_size / 1024, 2),
                        'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        'is_wscad': True,
                        'is_emri_no': project_info.get('is_emri_no', ''),
                        'proje_adi': project_info.get('proje_adi', ''),
                        'revizyon_no': project_info.get('revizyon_no', ''),
                        'project_info': project_info
                    })
            except Exception as e:
                print(f"Dosya bilgisi alınamadı {filepath}: {e}")
                continue
        
        wscad_files.sort(key=lambda x: x['modified'], reverse=True)
        return wscad_files
        
    except Exception as e:
        st.error(f"WSCAD dosya tarama hatası: {e}")
        return []

# User authentication
auth_status, username = authenticate()

if not setup_success:
    st.error("Veritabanı oluşturulamadı. Lütfen uygulama izinlerini kontrol edin.")
else:
    st.success("WSCAD BOM karşılaştırma sistemi hazır.")

# Main application logic
if auth_status:
    # Initialize session state
    if 'selected_files' not in st.session_state:
        st.session_state.selected_files = []
    
    if 'comparison_result' not in st.session_state:
        st.session_state.comparison_result = None

    if 'auto_comparison_result' not in st.session_state:
        st.session_state.auto_comparison_result = None

    if 'current_project_id' not in st.session_state:
        st.session_state.current_project_id = None

    # Main application
    st.title("WSCAD BOM Yönetimi ve Proje Takibi")

    # Sidebar
    with st.sidebar:
        st.header(f"Hoşgeldiniz, {username}")

        # Project Management Section
        st.subheader("🏗️ Proje Yönetimi")
        
        # Project selection/creation
        projects = db.get_all_projects()
        
        if projects:
            try:
                # Safely extract project names and revisions
                project_options = ["Yeni Proje Oluştur"]
                for p in projects:
                    if isinstance(p, tuple):
                        # If project is returned as tuple from database
                        proj_dict = {
                            'id': p[0],
                            'name': p[1],
                            'current_revision': p[2] if len(p) > 2 else 0
                        }
                    elif isinstance(p, dict):
                        # If project is already a dictionary
                        proj_dict = p
                    else:
                        continue
                        
                    project_name = proj_dict.get('name', 'Unnamed Project')
                    revision = proj_dict.get('current_revision', 0)
                    project_options.append(f"{project_name} (Rev: {revision})")
                    
                selected_project = st.selectbox("Proje Seçin", project_options)
                
                if selected_project != "Yeni Proje Oluştur":
                    # Extract project info from selection
                    project_name = selected_project.split(" (Rev:")[0]
                    selected_proj = next((p for p in projects if (isinstance(p, dict) and p.get('name') == project_name) or 
                                        (isinstance(p, tuple) and p[1] == project_name)), None)
                    
                    if selected_proj:
                        if isinstance(selected_proj, tuple):
                            st.session_state.current_project_id = selected_proj[0]
                        else:
                            st.session_state.current_project_id = selected_proj.get('id')
                            
                        # Show project info
                        st.success(f"✅ Aktif Proje: {project_name}")
            except Exception as e:
                st.error(f"Proje seçimi hatası: {str(e)}")
                st.session_state.current_project_id = None
        else:
            st.info("Henüz proje yok. Yeni proje oluşturun.")
            st.session_state.current_project_id = None

        # New project creation
        if st.session_state.current_project_id is None:
            with st.expander("➕ Yeni WSCAD Projesi Oluştur"):
                new_project_name = st.text_input("Proje Adı", key="new_project_name", 
                                                placeholder="örn: WSCAD_Proje_24057")
                new_project_desc = st.text_area("Proje Açıklaması", key="new_project_desc",
                                               placeholder="Bu projenin amacı ve kapsamı")
                
                if st.button("Proje Oluştur"):
                    if new_project_name:
                        try:
                            # Create project in local SQLite
                            project_id = db.create_project(new_project_name, new_project_desc, username)
                            
                            if project_id:
                                st.session_state.current_project_id = project_id
                                st.success(f"✅ Proje '{new_project_name}' oluşturuldu!")
                                
                                # Sync to Supabase if available
                                if supabase:
                                    try:
                                        supabase_project_id = supabase.create_wscad_project(
                                            new_project_name, 
                                            new_project_desc, 
                                            username, 
                                            project_id
                                        )
                                        
                                        if supabase_project_id:
                                            db.mark_project_synced_to_supabase(project_id, supabase_project_id)
                                            st.info("☁️ Proje Supabase'e kaydedildi")
                                    except Exception as e:
                                        st.warning(f"Supabase senkronizasyon hatası: {e}")
                            
                            db.log_activity(f"Yeni WSCAD projesi oluşturuldu: {new_project_name}", 
                                          username, project_id)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Proje oluşturma hatası: {str(e)}")
                    else:
                        st.warning("Proje adı gerekli")

        # Directory selection section
        st.subheader("📁 WSCAD Dosya Dizini")
        default_dirs = {
            "Downloads": os.path.expanduser("~/Downloads"),
            "Documents": os.path.expanduser("~/Documents"),
            "Desktop": os.path.expanduser("~/Desktop"),
            "Custom": "custom"
        }

        selected_dir_option = st.selectbox(
            "WSCAD dosya dizini seçin",
            options=list(default_dirs.keys()),
            index=0
        )

        if selected_dir_option == "Custom":
            directory = st.text_input("Özel dizin yolu girin", 
                                    placeholder="C:/WSCAD_Files/")
        else:
            directory = default_dirs[selected_dir_option]
            st.text(f"Seçilen dizin: {directory}")

        # Directory validation and WSCAD file scanning
        if directory and os.path.exists(directory):
            st.success(f"✅ Geçerli dizin: {directory}")
            
            if st.button("🔍 WSCAD Dosyalarını Tara"):
                with st.spinner("WSCAD BOM dosyaları taranıyor..."):
                    wscad_files = scan_xlsx_files(directory)
                    st.session_state.wscad_files = wscad_files
                    if wscad_files:
                        st.success(f"📊 {len(wscad_files)} adet WSCAD BOM dosyası bulundu")
                        db.log_activity(f"WSCAD directory scanned: {directory}, found {len(wscad_files)} files", 
                                      username, st.session_state.current_project_id)
                        
                        # Dosyaları yerel veritabanına ekle
                        for wscad_file in wscad_files:
                            db.add_wscad_file(
                                wscad_file['filename'],
                                wscad_file['filepath'],
                                wscad_file['size_kb'] * 1024,
                                wscad_file['project_info']
                            )
                    else:
                        st.warning("Bu dizinde WSCAD BOM dosyası bulunamadı")
        else:
            st.error("❌ Geçersiz dizin yolu")

        # Help section - WSCAD'a özel
        with st.expander("📚 WSCAD BOM Nasıl Kullanılır?"):
            st.markdown("""
            ### WSCAD BOM Karşılaştırma Rehberi
            1. **Proje Seçimi**: WSCAD projenizi seçin veya yeni proje oluşturun
            2. **Dizin Seçimi**: WSCAD BOM Excel dosyalarınızın bulunduğu dizini seçin
            3. **Dosya Tarama**: Sistem otomatik olarak WSCAD formatındaki dosyaları bulur
            4. **Karşılaştırma**: İki farklı revizyon dosyasını karşılaştırın
            5. **Proje Kayıt**: Sonuçları projeye revizyon olarak kaydedin
            6. **Senkronizasyon**: Veriler otomatik olarak Supabase'e kaydedilir
            
            #### WSCAD Sütun Destekleri:
            - POZ NO, PARCA NO, PARCA ADI
            - TOPLAM ADET, BİRİM ADET
            - STOK KODU, MALZEME, AÇIKLAMA
            """)

        # Logout button
        if st.button("Çıkış"):
            db.log_activity("Kullanıcı çıkış yaptı", username)
            st.session_state.clear()
            st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "WSCAD Dosyalar", "BOM Karşılaştırma", "Otomatik Karşılaştırma", 
        "Proje Revizyonları", "Geçmiş", "ERP'ye Aktar"
    ])

    # WSCAD Files tab
    with tab1:
        st.header("WSCAD BOM Dosyaları")

        if 'wscad_files' not in st.session_state or not st.session_state.wscad_files:
            st.info("WSCAD BOM dosyalarını görmek için yan panelden dizin seçin ve 'WSCAD Dosyalarını Tara' butonuna tıklayın.")
        else:
            st.subheader(f"📁 Bulunan WSCAD BOM Dosyaları ({len(st.session_state.wscad_files)} adet)")

            # WSCAD dosya listesi ile detaylı bilgi
            selected_files = []
            
            for idx, file in enumerate(st.session_state.wscad_files):
                with st.expander(f"📄 {file['filename']}", expanded=False):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    with col1:
                        st.write(f"**İş Emri No:** {file.get('is_emri_no', 'N/A')}")
                        st.write(f"**Proje Adı:** {file.get('proje_adi', 'N/A')}")
                        st.write(f"**Revizyon:** {file.get('revizyon_no', 'N/A')}")
                    
                    with col2:
                        st.write(f"**Dosya Boyutu:** {file['size_kb']} KB")
                        st.write(f"**Değiştirilme:** {file['modified']}")
                        st.write(f"**Dizin:** {os.path.dirname(file['filepath'])}")
                    
                    with col3:
                        if st.checkbox("Seç", key=f"wscad_select_{idx}"):
                            selected_files.append(file)

            st.session_state.selected_files = selected_files

            # Show selected files count and comparison
            if selected_files:
                st.info(f"Seçili WSCAD dosya sayısı: {len(selected_files)}")
                
                if len(selected_files) == 2:
                    st.success("✅ BOM karşılaştırması için 2 dosya seçildi")
                    
                    # Dosya bilgilerini göster
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Dosya 1 (Eski Revizyon):**")
                        st.write(f"📄 {selected_files[0]['filename']}")
                        st.write(f"🏗️ İş Emri: {selected_files[0].get('is_emri_no', 'N/A')}")
                        st.write(f"📝 Rev: {selected_files[0].get('revizyon_no', 'N/A')}")
                    
                    with col2:
                        st.write("**Dosya 2 (Yeni Revizyon):**")
                        st.write(f"📄 {selected_files[1]['filename']}")
                        st.write(f"🏗️ İş Emri: {selected_files[1].get('is_emri_no', 'N/A')}")
                        st.write(f"📝 Rev: {selected_files[1].get('revizyon_no', 'N/A')}")
                    
                    # Karşılaştırma butonu
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 WSCAD BOM Karşılaştır", help="WSCAD BOM dosyalarını karşılaştır"):
                            with st.spinner("📊 WSCAD BOM dosyaları karşılaştırılıyor..."):
                                try:
                                    file1 = selected_files[0]
                                    file2 = selected_files[1]
                                    
                                    comparison_result = excel_processor.compare_excel_files(
                                        file1['filepath'],
                                        file2['filepath'],
                                        username
                                    )
                                    
                                    st.session_state.comparison_result = comparison_result
                                    st.session_state.file1_info = file1
                                    st.session_state.file2_info = file2
                                    
                                    st.success(f"WSCAD BOM karşılaştırması tamamlandı!")
                                    st.info(f"Toplam {len(comparison_result)} BOM değişikliği bulundu")
                                    
                                    db.log_activity(f"WSCAD BOM compared: {file1['filename']} vs {file2['filename']}", 
                                                   username, st.session_state.current_project_id, 
                                                   {'file1': file1['filename'], 'file2': file2['filename']})
                                    
                                except Exception as e:
                                    st.error(f"WSCAD BOM karşılaştırma hatası: {str(e)}")
                    
                    with col2:
                        if (st.session_state.comparison_result and 
                            st.session_state.current_project_id and 
                            supabase and supabase.connection):
                            if st.button("💾 Karşılaştırmayı Projeye Kaydet", 
                                       help="WSCAD BOM karşılaştırmasını aktif projeye revizyon olarak kaydet"):
                                
                                try:
                                    # Save comparison to local DB first
                                    comparison_id = db.save_comparison_result(
                                        file1_id=st.session_state.file1_info.get('id'),
                                        file2_id=st.session_state.file2_info.get('id'),
                                        project_id=st.session_state.current_project_id,
                                        changes_count=len(st.session_state.comparison_result)
                                    )
                                    
                                    if comparison_id and supabase and supabase.connection:
                                        # Then sync to Supabase
                                        supabase_id = supabase.save_wscad_comparison_to_project(
                                            st.session_state.current_project_id,
                                            st.session_state.comparison_result,
                                            st.session_state.file1_info['filename'],
                                            st.session_state.file2_info['filename'],
                                            st.session_state.file1_info.get('project_info'),
                                            st.session_state.file2_info.get('project_info'),
                                            username
                                        )
                                        
                                        if supabase_id:
                                            db.mark_comparison_synced_to_supabase(comparison_id, supabase_id)
                                            st.success("✅ Karşılaştırma sonuçları kaydedildi ve senkronize edildi")
                                        else:
                                            st.warning("Yerel kayıt başarılı fakat Supabase senkronizasyonu başarısız")
                                    else:
                                        st.error("Karşılaştırma sonuçları kaydedilemedi")

                                except Exception as e:
                                    st.error(f"Kaydetme hatası: {str(e)}")

                elif len(selected_files) > 2:
                    st.warning("⚠️ BOM karşılaştırması için sadece 2 dosya seçin")
                else:
                    st.info("ℹ️ BOM karşılaştırması için 2 WSCAD dosyası seçin")

            # Display WSCAD BOM comparison results if available
            if st.session_state.comparison_result:
                st.subheader("📊 WSCAD BOM Karşılaştırma Sonuçları")
                
                if not st.session_state.comparison_result:
                    st.success("✅ WSCAD BOM dosyaları arasında fark bulunamadı")
                else:
                    diff_df = pd.DataFrame(st.session_state.comparison_result)
                    
                    # WSCAD BOM özet metrikleri
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Toplam Değişiklik", len(diff_df))
                    with col2:
                        modified_count = len(diff_df[diff_df['change_type'] == 'modified']) if 'change_type' in diff_df.columns else 0
                        st.metric("🔄 Değişen Alanlar", modified_count)
                    with col3:
                        added_count = len(diff_df[diff_df['change_type'] == 'added']) if 'change_type' in diff_df.columns else 0
                        st.metric("➕ Eklenen Kalemler", added_count)
                    with col4:
                        removed_count = len(diff_df[diff_df['change_type'] == 'removed']) if 'change_type' in diff_df.columns else 0
                        st.metric("➖ Silinen Kalemler", removed_count)
                    with col5:
                        quantity_changes = len(diff_df[diff_df['change_type'].str.contains('quantity', na=False)]) if 'change_type' in diff_df.columns else 0
                        st.metric("📊 Miktar Değişiklikleri", quantity_changes)
                    
                    # Değişiklik türlerine göre filtreleme
                    change_types = diff_df['change_type'].unique() if 'change_type' in diff_df.columns else []
                    selected_change_type = st.selectbox("Değişiklik türüne göre filtrele:", 
                                                       ['Tümü'] + list(change_types))
                    
                    # Filtrelenmiş sonuçları göster
                    if selected_change_type != 'Tümü':
                        filtered_df = diff_df[diff_df['change_type'] == selected_change_type]
                    else:
                        filtered_df = diff_df
                    
                    st.dataframe(filtered_df, use_container_width=True)
                    
                    # WSCAD BOM raporu indirme
                    if st.button("📥 WSCAD BOM Karşılaştırma Raporunu İndir"):
                        try:
                            report_data = excel_processor.generate_comparison_report(st.session_state.comparison_result)
                            st.download_button(
                                label="Excel Raporu İndir",
                                data=report_data.getvalue(),
                                file_name=f"wscad_bom_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                help="WSCAD BOM karşılaştırma raporunu Excel dosyası olarak indir"
                            )
                        except Exception as e:
                            st.error(f"Rapor oluşturma hatası: {str(e)}")

    # BOM Comparison tab
    with tab2:
        st.header("Detaylı BOM Karşılaştırma")
        
        if st.session_state.comparison_result:
            # Karşılaştırma özeti
            st.subheader("📋 Karşılaştırma Özeti")
            
            if hasattr(st.session_state, 'file1_info') and hasattr(st.session_state, 'file2_info'):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**📄 Dosya 1 (Referans):**")
                    st.code(f"""
Dosya: {st.session_state.file1_info['filename']}
İş Emri: {st.session_state.file1_info.get('is_emri_no', 'N/A')}
Proje: {st.session_state.file1_info.get('proje_adi', 'N/A')}
Revizyon: {st.session_state.file1_info.get('revizyon_no', 'N/A')}
Tarih: {st.session_state.file1_info['modified']}
                    """)
                
                with col2:
                    st.write("**📄 Dosya 2 (Güncel):**")
                    st.code(f"""
Dosya: {st.session_state.file2_info['filename']}
İş Emri: {st.session_state.file2_info.get('is_emri_no', 'N/A')}
Proje: {st.session_state.file2_info.get('proje_adi', 'N/A')}
Revizyon: {st.session_state.file2_info.get('revizyon_no', 'N/A')}
Tarih: {st.session_state.file2_info['modified']}
                    """)
            if st.button("💾 Karşılaştırmayı Kaydet"):
                try:
                    with st.spinner("Karşılaştırma kaydediliyor..."):
                        comparison_id = supabase.save_wscad_comparison_to_project(
                            project_id=st.session_state.current_project_id,
                            comparison_data=st.session_state.comparison_result,
                            file1_name=st.session_state.file1_info['filename'],
                            file2_name=st.session_state.file2_info['filename'],
                            file1_info=st.session_state.file1_info,
                            file2_info=st.session_state.file2_info,
                            created_by=username
                        )
                        
                        if comparison_id:
                            st.success("✅ Karşılaştırma başarıyla kaydedildi!")
                            st.session_state.last_saved_comparison = comparison_id
                        else:
                            st.error("Kaydetme başarısız oldu")
                            
                except Exception as e:
                    st.error(f"Kaydetme hatası: {str(e)}")
            # Değişiklik analizi
            st.subheader("🔍 Değişiklik Analizi")
            
            diff_df = pd.DataFrame(st.session_state.comparison_result)
            
            # POZ NO bazında gruplandırma
            if 'poz_no' in diff_df.columns:
                poz_changes = diff_df.groupby('poz_no').size().reset_index(name='change_count')
                poz_changes = poz_changes.sort_values('change_count', ascending=False).head(10)
                
                if not poz_changes.empty:
                    st.write("**En Çok Değişiklik Olan POZ Numaraları:**")
                    fig = px.bar(poz_changes, x='poz_no', y='change_count', 
                               title="POZ NO Bazında Değişiklik Sayısı")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Sütun bazında değişiklik analizi
            if 'column' in diff_df.columns:
                column_changes = diff_df.groupby('column').size().reset_index(name='change_count')
                column_changes = column_changes.sort_values('change_count', ascending=False)
                
                if not column_changes.empty:
                    st.write("**Sütun Bazında Değişiklik Dağılımı:**")
                    fig = px.pie(column_changes, values='change_count', names='column',
                               title="Hangi Sütunlarda Daha Çok Değişiklik Var?")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Kritik değişiklikler
            if 'severity' in diff_df.columns:
                critical_changes = diff_df[diff_df['severity'] == 'high']
                if not critical_changes.empty:
                    st.subheader("🚨 Kritik Değişiklikler")
                    st.dataframe(critical_changes, use_container_width=True)
        else:
            st.info("Detaylı analiz için önce bir karşılaştırma yapın.")

    # Auto-Compare tab
    with tab3:
        st.header("Otomatik WSCAD BOM Karşılaştırma")
        st.write("En son değiştirilen iki WSCAD BOM dosyasını otomatik olarak karşılaştırır.")

        if 'wscad_files' not in st.session_state or len(st.session_state.wscad_files) < 2:
            st.warning("⚠️ Otomatik karşılaştırma için en az 2 WSCAD BOM dosyası gerekli")
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**En son WSCAD BOM dosyaları:**")
                file1 = st.session_state.wscad_files[0]
                file2 = st.session_state.wscad_files[1]
                
                st.write(f"1️⃣ **{file1['filename']}**")
                st.caption(f"İş Emri: {file1.get('is_emri_no', 'N/A')} | Rev: {file1.get('revizyon_no', 'N/A')}")
                
                st.write(f"2️⃣ **{file2['filename']}**")
                st.caption(f"İş Emri: {file2.get('is_emri_no', 'N/A')} | Rev: {file2.get('revizyon_no', 'N/A')}")
            
            with col2:
                if st.button("🚀 Otomatik WSCAD BOM Karşılaştır"):
                    with st.spinner("En son WSCAD BOM dosyaları karşılaştırılıyor..."):
                        try:
                            comparison_result = excel_processor.compare_excel_files(
                                file1['filepath'],
                                file2['filepath'],
                                username
                            )
                            
                            auto_result = {
                                'file1': file1,
                                'file2': file2,
                                'comparison_data': comparison_result,
                                'comparison_count': len(comparison_result)
                            }
                            
                            st.session_state.auto_comparison_result = auto_result
                            
                            st.success(f"✅ Otomatik WSCAD BOM karşılaştırması tamamlandı!")
                            st.info(f"📊 Toplam {len(comparison_result)} BOM değişikliği bulundu")
                            
                            db.log_activity(f"Auto-compared WSCAD BOM: {file1['filename']} vs {file2['filename']}", 
                                          username, st.session_state.current_project_id)
                            
                        except Exception as e:
                            st.error(f"❌ Otomatik WSCAD BOM karşılaştırma hatası: {str(e)}")

            # Display auto-comparison results and save option
            if st.session_state.auto_comparison_result:
                st.subheader("🎯 Otomatik WSCAD BOM Karşılaştırma Sonuçları")
                
                result = st.session_state.auto_comparison_result
                st.write(f"📄 **Dosya 1:** {result['file1']['filename']}")
                st.write(f"📄 **Dosya 2:** {result['file2']['filename']}")
                st.write(f"📊 **BOM Değişiklik Sayısı:** {result['comparison_count']}")
                
                # Save to project button
                if (st.session_state.current_project_id and 
                    supabase and supabase.connection):
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("💾 Otomatik Karşılaştırmayı Projeye Kaydet"):
                            try:
                                comparison_id = supabase.save_wscad_comparison_to_project(
                                    st.session_state.current_project_id,
                                    result['comparison_data'],
                                    result['file1']['filename'],
                                    result['file2']['filename'],
                                    result['file1'].get('project_info'),
                                    result['file2'].get('project_info'),
                                    username
                                )
                                
                                if comparison_id:
                                    st.success("✅ Otomatik karşılaştırma projeye kaydedildi!")
                                    db.log_activity(f"Auto-comparison saved to project (ID: {comparison_id})", 
                                                   username, st.session_state.current_project_id)
                                else:
                                    st.error("Kaydetme hatası")
                            except Exception as e:
                                st.error(f"Kaydetme hatası: {str(e)}")
                    
                    with col2:
                        if st.button("📥 Otomatik Karşılaştırma Raporunu İndir"):
                            try:
                                report_data = excel_processor.generate_comparison_report(result['comparison_data'])
                                st.download_button(
                                    label="Excel Raporu İndir",
                                    data=report_data.getvalue(),
                                    file_name=f"auto_wscad_bom_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                            except Exception as e:
                                st.error(f"Rapor oluşturma hatası: {str(e)}")
                
                if result['comparison_data']:
                    comparison_df = pd.DataFrame(result['comparison_data'])
                    st.dataframe(comparison_df, use_container_width=True)

    # Project Revisions tab
    with tab4:
        st.header("Proje Revizyonları ve İstatistikler")
        
        if not st.session_state.current_project_id:
            st.warning("📋 Proje revizyonlarını görmek için önce bir proje seçin")
        elif not supabase or not supabase.connection:
            st.error("❌ Supabase bağlantısı yok - revizyonlar görüntülenemiyor")
        else:
            # Project info
            projects = supabase.get_wscad_projects()
            current_project = None
            
            # Find current project in Supabase
            local_project = db.get_project_by_id(st.session_state.current_project_id)
            if local_project and local_project.get('supabase_id'):
                current_project = next((p for p in projects if p['id'] == local_project['supabase_id']), None)
            
            if current_project:
                st.subheader(f"📊 Proje: {current_project['name']}")
                st.write(f"**Açıklama:** {current_project['description']}")
                st.write(f"**Oluşturan:** {current_project['created_by']}")
                st.write(f"**Oluşturulma:** {current_project['created_at']}")
                
                # Project statistics
                stats = supabase.get_wscad_project_statistics(current_project['id'])
                if stats and stats['general']:
                    st.subheader("📈 Proje İstatistikleri")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Toplam Karşılaştırma", stats['general']['total_comparisons'] or 0)
                    with col2:
                        st.metric("Toplam Değişiklik", stats['general']['total_changes'] or 0)
                    with col3:
                        st.metric("Eklenen Kalemler", stats['general']['total_added_items'] or 0)
                    with col4:
                        st.metric("Silinen Kalemler", stats['general']['total_removed_items'] or 0)
                
                # Project comparisons
                comparisons = supabase.get_wscad_project_comparisons(current_project['id'])
                if comparisons:
                    st.subheader("🔍 Proje Karşılaştırma Geçmişi")
                    
                    for comp in comparisons:
                        with st.expander(f"Rev {comp['revision_number']}: {comp['comparison_title']} ({comp['changes_count']} değişiklik)"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write(f"**Dosya 1:** {comp['file1_name']}")
                                st.write(f"**İş Emri:** {comp.get('file1_is_emri_no', 'N/A')}")
                                st.write(f"**Revizyon:** {comp.get('file1_revizyon_no', 'N/A')}")
                            
                            with col2:
                                st.write(f"**Dosya 2:** {comp['file2_name']}")
                                st.write(f"**İş Emri:** {comp.get('file2_is_emri_no', 'N/A')}")
                                st.write(f"**Revizyon:** {comp.get('file2_revizyon_no', 'N/A')}")
                            
                            st.write(f"**Karşılaştırma Tarihi:** {comp['created_at']}")
                            st.write(f"**Değişiklik Sayısı:** {comp['changes_count']}")
                            st.write(f"**Oluşturan:** {comp['created_by']}")
                            
                            if st.button(f"Detayları Göster", key=f"details_{comp['id']}"):
                                details = supabase.get_wscad_comparison_details(comp['id'])
                                if details:
                                    st.write("**Değişiklik Detayları:**")
                                    if details['changes']:
                                        changes_df = pd.DataFrame(details['changes'])
                                        st.dataframe(changes_df, use_container_width=True)
                                    
                                    if details['quantity_changes']:
                                        st.write("**Miktar Değişiklikleri:**")
                                        qty_df = pd.DataFrame(details['quantity_changes'])
                                        st.dataframe(qty_df, use_container_width=True)
                else:
                    st.info("Bu proje için henüz karşılaştırma kaydı bulunmuyor")
            else:
                st.warning("Proje Supabase'de bulunamadı. Lütfen projeyi senkronize edin.")

    # History tab
    with tab5:
        st.header("Sistem Geçmişi")

        tab5_1, tab5_2 = st.tabs(["Etkinlik Geçmişi", "Yerel Dosya Geçmişi"])

        with tab5_1:
            # Activity logs from local database
            activity_logs = db.get_activity_logs()

            if not activity_logs:
                st.info("Henüz etkinlik kaydedilmedi")
            else:
                logs_df = pd.DataFrame(activity_logs, columns=["ID", "User", "Activity", "Timestamp", "Project_ID", "File_Info"])
                
                # Filter by current project if selected
                if st.session_state.current_project_id:
                    project_logs = logs_df[logs_df['Project_ID'] == st.session_state.current_project_id]
                    if not project_logs.empty:
                        st.subheader("Aktif Proje Etkinlikleri")
                        st.dataframe(project_logs, use_container_width=True)
                
                st.subheader("Tüm Etkinlikler")
                st.dataframe(logs_df, use_container_width=True)

                # Activity visualization
                if len(logs_df) > 0:
                    activity_counts = logs_df["Activity"].apply(
                        lambda x: "WSCAD BOM İşlemleri" if "wscad" in x.lower() or "bom" in x.lower() else
                                  "Karşılaştırma" if "compar" in x.lower() else
                                  "Proje Yönetimi" if "proje" in x.lower() or "project" in x.lower() else
                                  "Dosya İşlemleri" if "dosya" in x.lower() or "file" in x.lower() else
                                  "Diğer"
                    ).value_counts().reset_index()
                    activity_counts.columns = ["Activity Type", "Count"]

                    fig = px.pie(activity_counts, values="Count", names="Activity Type", 
                                 title="WSCAD BOM Sistem Faaliyet Dağılımı")
                    st.plotly_chart(fig, use_container_width=True)

        with tab5_2:
            # Local WSCAD file history
            recent_files = db.get_recent_wscad_files(10)
            
            if recent_files:
                st.subheader("Son İşlenen WSCAD BOM Dosyaları")
                
                files_data = []
                for file in recent_files:
                    # Convert sqlite3.Row to dict
                    file_dict = dict(file)
                    files_data.append({
                        'Dosya Adı': file_dict['filename'],
                        'İş Emri No': file_dict.get('is_emri_no', 'N/A'),
                        'Proje Adı': file_dict.get('proje_adi', 'N/A'),
                        'Boyut (KB)': file_dict['filesize'] // 1024 if file_dict['filesize'] else 0,
                        'Tarih': file_dict['detected_time']
                    })
                
                files_df = pd.DataFrame(files_data)
                st.dataframe(files_df, use_container_width=True)
            else:
                st.info("Henüz WSCAD BOM dosyası işlenmedi")

    # Export to ERP tab
    with tab6:
        st.header("ERP'ye Aktar")

        # Önce hangi veri tipinin export edileceğini belirle
        export_source = st.radio("Hangi veriyi ERP'ye aktarmak istiyorsunuz?", [
            "Mevcut Karşılaştırma Sonucu",
            "Proje Revizyonu",
            "Otomatik Karşılaştırma Sonucu"
        ])

        export_data = None
        export_metadata = None

        if export_source == "Mevcut Karşılaştırma Sonucu" and st.session_state.comparison_result:
            export_data = st.session_state.comparison_result
            export_metadata = {
                'source': 'manual_comparison',
                'file1': getattr(st.session_state, 'file1_info', {}),
                'file2': getattr(st.session_state, 'file2_info', {}),
                'project_id': st.session_state.current_project_id
            }
        elif export_source == "Otomatik Karşılaştırma Sonucu" and st.session_state.auto_comparison_result:
            result = st.session_state.auto_comparison_result
            export_data = result['comparison_data']
            export_metadata = {
                'source': 'auto_comparison',
                'file1': result['file1'],
                'file2': result['file2'],
                'project_id': st.session_state.current_project_id
            }
        elif export_source == "Proje Revizyonu":
            if st.session_state.current_project_id and supabase and supabase.connection:
                # Get project comparisons for selection
                local_project = db.get_project_by_id(st.session_state.current_project_id)
                if local_project and local_project.get('supabase_id'):
                    comparisons = supabase.get_wscad_project_comparisons(local_project['supabase_id'])
                    
                    if comparisons:
                        comp_options = [f"Rev {c['revision_number']}: {c['comparison_title']}" for c in comparisons]
                        selected_comp = st.selectbox("Hangi revizyonu export etmek istiyorsunız?", comp_options)
                        
                        if selected_comp:
                            comp_index = comp_options.index(selected_comp)
                            selected_comparison = comparisons[comp_index]
                            
                            # Get detailed comparison data
                            details = supabase.get_wscad_comparison_details(selected_comparison['id'])
                            if details and details['changes']:
                                export_data = details['changes']
                                export_metadata = {
                                    'source': 'project_revision',
                                    'revision_number': selected_comparison['revision_number'],
                                    'comparison_title': selected_comparison['comparison_title'],
                                    'project_id': st.session_state.current_project_id
                                }
                    else:
                        st.warning("Bu projede henüz karşılaştırma bulunmuyor")
            else:
                st.warning("Proje revizyonu export için proje seçin ve Supabase bağlantısını kontrol edin")

        if export_data:
            st.success(f"✅ Export için {len(export_data)} kayıt hazır")
            
            # ERP connection settings
            with st.expander("ERP Bağlantı Ayarları"):
                erp_host = st.text_input("ERP Host", value="localhost")
                erp_port = st.number_input("ERP Port", value=5432)
                erp_db = st.text_input("ERP Database", value="erp_database")
                erp_user = st.text_input("ERP Username", value="erp_user")
                erp_password = st.text_input("ERP Password", type="password")

            # Export options
            export_format = st.selectbox("Export Format", ["JSON", "CSV", "Direct DB Connection"])
            include_metadata = st.checkbox("WSCAD Metadata Dahil Et", value=True)

            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🚀 ERP'ye Aktar"):
                    with st.spinner("WSCAD BOM verileri ERP'ye aktarılıyor..."):
                        try:
                            connection_params = {
                                "host": erp_host,
                                "port": erp_port,
                                "database": erp_db,
                                "user": erp_user,
                                "password": erp_password,
                                "format": export_format.lower()
                            }

                            export_result = erp_exporter.export_to_erp(
                                export_data,
                                connection_params,
                                include_metadata,
                                file_info=export_metadata
                            )

                            db.log_activity(f"WSCAD BOM data exported to ERP in {export_format} format", 
                                          username, st.session_state.current_project_id)

                            st.success(f"✅ WSCAD BOM verileri başarıyla ERP'ye aktarıldı: {export_result}")
                        except Exception as e:
                            st.error(f"❌ ERP'ye aktarmada hata: {str(e)}")
                            db.log_activity(f"ERP export failed: {str(e)}", username, st.session_state.current_project_id)
            
            with col2:
                # Manual export - JSON/CSV download
                if st.button("📥 Dosya Olarak İndir"):
                    try:
                        if export_format == "JSON":
                            export_content = json.dumps({
                                'metadata': export_metadata,
                                'data': export_data
                            }, ensure_ascii=False, indent=2, default=str)
                            
                            st.download_button(
                                "JSON Dosyasını İndir",
                                export_content,
                                file_name=f"wscad_bom_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                mime="application/json"
                            )
                        
                        elif export_format == "CSV":
                            export_df = pd.DataFrame(export_data)
                            csv_content = export_df.to_csv(index=False, encoding='utf-8-sig')
                            
                            st.download_button(
                                "CSV Dosyasını İndir",
                                csv_content,
                                file_name=f"wscad_bom_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                            
                    except Exception as e:
                        st.error(f"Dosya oluşturma hatası: {str(e)}")

            # Data preview
            st.subheader("📋 Export Edilecek Veri Önizlemesi")
            
            if export_metadata:
                st.write("**Metadata:**")
                st.json(export_metadata)
            
            st.write("**WSCAD BOM Karşılaştırma Verileri:**")
            preview_df = pd.DataFrame(export_data)
            st.dataframe(preview_df.head(10), use_container_width=True)
            
            if len(export_data) > 10:
                st.caption(f"Önizleme: İlk 10 kayıt gösteriliyor (Toplam: {len(export_data)} kayıt)")

        else:
            st.info("Export için önce bir karşılaştırma yapın veya proje revizyonu seçin")

            # Show available data sources
            st.subheader("📊 Mevcut Veri Kaynakları")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                has_comparison = bool(st.session_state.comparison_result)
                st.metric("Manuel Karşılaştırma", "✅ Hazır" if has_comparison else "❌ Yok")
            
            with col2:
                has_auto = bool(st.session_state.auto_comparison_result)
                st.metric("Otomatik Karşılaştırma", "✅ Hazır" if has_auto else "❌ Yok")
            
            with col3:
                has_project = bool(st.session_state.current_project_id)
                st.metric("Aktif Proje", "✅ Seçili" if has_project else "❌ Seçilmedi")

        # ERP Export History
        st.subheader("📜 ERP Export Geçmişi")
        
        # Get export activities from logs
        all_logs = db.get_activity_logs(50)
        export_logs = [log for log in all_logs if 'export' in log[2].lower() or 'erp' in log[2].lower()]
        
        if export_logs:
            export_df = pd.DataFrame(export_logs, columns=["ID", "User", "Activity", "Timestamp", "Project_ID", "File_Info"])
            st.dataframe(export_df, use_container_width=True)
        else:
            st.info("Henüz ERP export işlemi yapılmamış")

else:
    st.warning("Sisteme erişmek için lütfen giriş yapın")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <small>WSCAD BOM Comparison System v2.0 | Proje Yönetimi & Supabase Entegrasyonu</small>
</div>
""", unsafe_allow_html=True)
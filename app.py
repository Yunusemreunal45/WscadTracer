import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import threading

# Import custom modules
from auth import authenticate
from database import Database
from excel_processor import ExcelProcessor
from file_monitor import FileMonitor
from erp_exporter import ERPExporter
from utils import get_file_info, log_activity

# Page configuration
st.set_page_config(
    page_title="WSCAD Bom Comparison System",
    page_icon="📊",
    layout="wide"
)

# Initialize database using SQLite
@st.cache_resource
def get_database():
    """Singleton veri tabanı oluştur (thread güvenliği için)"""
    db = Database()
    setup_success = db.setup_database()
    return db, setup_success

db, setup_success = get_database()

if not setup_success:
    st.error("Veritabanı oluşturulamadı. Lütfen uygulama izinlerini kontrol edin.")
else:
    st.success("WSCAD Bom karşılaştırma sistemi hazır.")

# Initialize Excel processor
excel_processor = ExcelProcessor()

# Initialize ERP exporter
erp_exporter = ERPExporter()

# User authentication
auth_status, username = authenticate()

# This will hold our file monitor in a separate thread
file_monitor_thread = None
file_monitor = None

def start_monitoring(directory):
    """Start monitoring the specified directory as a background service"""
    global file_monitor

    try:
        # Create and start file monitor if not already created
        if file_monitor is None:
            file_monitor = FileMonitor(directory, db, excel_processor)
            file_monitor.start_monitoring()
            st.session_state.monitoring = True
            log_activity(f"Started background monitoring for directory: {directory}", db, username)
            return True
        return False
    except Exception as e:
        st.error(f"İzleme başlatma hatası: {e}")
        return False

def stop_monitoring():
    """Stop the file monitoring thread"""
    global file_monitor
    if file_monitor:
        file_monitor.stop_monitoring()
        st.session_state.monitoring = False
        log_activity("Stopped directory monitoring", db, username)

def auto_compare_latest_files(directory='.'):
    """Find and compare the two most recent Bom files"""
    try:
        # Use the Excel processor to find and compare latest files
        comparison_result = excel_processor.auto_compare_latest_files(directory)

        # Log the activity
        log_activity(f"Otomatik olarak karşılaştırılan son dosyalar: {os.path.basename(comparison_result['file1']['filepath'])} and {os.path.basename(comparison_result['file2']['filepath'])}", db, username)

        return comparison_result
    except Exception as e:
        st.error(f"Otomatik karşılaştırma hatası: {str(e)}")
        log_activity(f"Otomatik karşılaştırma hatası: {str(e)}", db, username)
        return None

# Main application logic
if auth_status:
    # Initialize session state for monitoring status
    if 'monitoring' not in st.session_state:
        st.session_state.monitoring = False

    # Initialize session state for selected files
    if 'selected_file' not in st.session_state:
        st.session_state.selected_file = None

    if 'selected_revision' not in st.session_state:
        st.session_state.selected_revision = None

    if 'comparison_result' not in st.session_state:
        st.session_state.comparison_result = None

    if 'auto_comparison_result' not in st.session_state:
        st.session_state.auto_comparison_result = None

    # Main application
    st.title("Wscad Bom Yönetimi ")

    # Help/Guide section
    with st.sidebar:
        with st.expander("📚 Nasıl Kullanılır?"):
            st.markdown("""
            ### Hızlı Başlangıç Rehberi
            1. **Dosya Seçimi**: 'Files' sekmesinden Bom dosyalarınızı seçin
            2. **Karşılaştırma**: İki dosyayı seçip karşılaştırın
            3. **Sonuçlar**: Değişiklikleri detaylı raporlarda görüntüleyin

            ### İpuçları
            - Otomatik karşılaştırma için 'Auto-Compare' kullanın
            - Revizyon geçmişini 'History' sekmesinde takip edin
            - ERP'ye aktarım için 'Export to ERP' sekmesini kullanın
            """)

        # System status indicators
        st.markdown("### 📊 Sistem Durumu")
        if st.session_state.monitoring:
            st.success("✅ Dizin İzleme Aktif")
        else:
            st.warning("⚠️ Dizin İzleme Pasif")

    # Sidebar
    with st.sidebar:
        st.header(f"Hoşlgeldiniz, {username}")

        # Directory monitoring section
        st.subheader("Dizin İzleme")

        st.subheader("İş Emri Bilgisi")
        is_emri_no = st.text_input("İş Emri No")
        # Default directory options
        default_dirs = {
            "Downloads": os.path.expanduser("~/Downloads"),
            "Documents": os.path.expanduser("~/Documents"),
            "Desktop": os.path.expanduser("~/Desktop"),
            "Custom": "custom"
        }

        selected_dir_option = st.selectbox(
            "İzlenecek dizini seçin",
            options=list(default_dirs.keys()),
            index=0
        )

        if selected_dir_option == "Custom":
            directory = st.text_input("Özel dizin yolu girin", help="Bom dosyalarının bulunduğu dizini girin")
        else:
            directory = default_dirs[selected_dir_option]
            st.text(f"Seçilen dizin: {directory}")

        # Directory validation
        is_valid_dir = os.path.exists(directory) if directory else False

        if is_valid_dir:
            st.success(f"✅ Geçerli dizin: {directory}")
            excel_files = excel_processor.list_excel_files(directory)
           # if excel_files:
            #    st.info(f"📊 Bu dizinde {len(excel_files)} Bom dosyası bulundu")
             #   for file in excel_files[:5]:  # Son 5 dosyayı göster
              #       st.text(f"📑 {file['filename']} - {file['modified']}")

        col1, col2 = st.columns(2)
        with col1:
            if not st.session_state.monitoring:
                start_button = st.button(
                    "İzlemeyi Başlat",
                    disabled=not is_valid_dir
                )
                if start_button:
                    if is_valid_dir:
                        start_monitoring(directory)
                    else:
                        st.error("Geçerli bir dizin seçin")

        with col2:
            if st.session_state.monitoring:
                if st.button("İzlemeyi Durdur"):
                    stop_monitoring()

        # Show monitoring status and current directory
        if st.session_state.monitoring:
            st.success(f"Dizin izleme aktif: {file_monitor.get_monitored_directory() if file_monitor else directory}")

            # Show recent file activities
            recent_files = db.get_recent_files(limit=5)
           # if recent_files:
            #    st.subheader("Son Dosya Aktiviteleri")
             #   for file in recent_files:
              #      st.text(f"📄 {file[1]} - {file[4]}")
        else:
            st.warning("Dizin izleme aktif değil")

        # Auto-comparison button (New Feature)
        st.subheader("Hızlı Otomatik karşılaştırma")
        if st.button("Son Karşılaştırılan Bom Dosyaları"):
            with st.spinner("En son Bom dosyalarını bulup karşılaştırıyoruz..."):
                try:
                    latest_files = db.get_recent_files(2)
                    if len(latest_files) >= 2:
                        file1_data = latest_files[0]
                        file2_data = latest_files[1]

                        comparison_result = excel_processor.compare_excel_files(
                            file1_data['filepath'],
                            file2_data['filepath']
                        )

                        auto_result = {
                            'file1': file1_data,
                            'file2': file2_data,
                            'comparison_data': comparison_result,
                            'comparison_count': len(comparison_result)
                        }

                        st.session_state.auto_comparison_result = auto_result

                        # Save to database with full data
                        comparison_id = db.save_comparison_result(
                            file2_data['id'],
                            file1_data['id'],
                            file2_data['id'],
                            len(comparison_result),
                            datetime.now(),
                            comparison_result
                        )

                        if comparison_id:
                            st.success(f"Karşılaştırma tamamlandı!")
                            st.info(f"Toplam {len(comparison_result)} fark bulundu:")
                            st.write(f"- Dosya 1: {os.path.basename(file1_data['filepath'])}")
                            st.write(f"- Dosya 2: {os.path.basename(file2_data['filepath'])}")
                    else:
                        st.warning("En az iki dosya gerekli")
                except Exception as e:
                    st.error(f"Otomatik karşılaştırma hatası: {str(e)}")

        # Logout button
        if st.button("Çıkış"):
            log_activity("Kullanıcı Çıkış yaptı", db, username)
            st.session_state.clear()
            st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Dosyalar", "Karşılaştırma", "Otomatik Karşılaştırma", "Tarih", "ERP'ye aktar"])

    # Files tab
    with tab1:
        st.header("Bom Dosyaları")

        # Add auto-refresh
        if 'refresh_counter' not in st.session_state:
            st.session_state.refresh_counter = 0

        # Her 1 saniyede bir yenile
        time.sleep(1)
        st.session_state.refresh_counter += 1

        # Fetch all files only if monitoring is active
        files = db.get_all_files() if st.session_state.monitoring else []

        if not files:
            if st.session_state.monitoring:
                st.info("Henüz hiçbir dosya algılanmadı. WSCAD Bom dosyalarını algılamak için bir dizini izlemeye başlayın.")
            else:
                st.info("İzleme durduruldu. Bom dosyalarını algılamak için izlemeyi başlatın.")
        else:
            # Create DataFrame from files
            files_df = pd.DataFrame(files, columns=["ID", "Filename", "Path", "Size (KB)", "Detected Time", "Processed", "Current Revision"])

            # Display files in a more organized way with actions
            st.subheader("📁 Tespit Edilen Bom Dosyalar")

            for _, row in files_df.iterrows():
                with st.expander(f"📄 {row['Filename']} - Rev.{row['Current Revision']}"):
                    col1, col2, col3 = st.columns([2,1,1])

                    with col1:
                        st.text(f"Size: {row['Size (KB)']} KB")
                        st.text(f"Detected: {row['Detected Time']}")

                    with col2:
                        if st.button("🗑️ Sil", key=f"del_{row['ID']}"):
                            # Delete file from database and disk
                            db.execute("DELETE FROM files WHERE id = ?", (row['ID'],))
                            if os.path.exists(row['Path']):
                                os.remove(row['Path'])
                            st.rerun()

                    with col3:
                        if st.button("💾 Arşiv", key=f"arch_{row['ID']}"):
                            # Move file to archive folder
                            archive_dir = "archived_files"
                            os.makedirs(archive_dir, exist_ok=True)
                            new_path = os.path.join(archive_dir, row['Filename'])
                            if os.path.exists(row['Path']):
                                import shutil
                                shutil.move(row['Path'], new_path)
                                db.execute("UPDATE files SET filepath = ? WHERE id = ?", (new_path, row['ID']))
                            st.rerun()

            # Allow user to select a file
            file_ids = [f[0] for f in files]
            file_names = [f[1] for f in files]
            selected_file_index = st.selectbox( "İşleme için bir dosya seçin (Dosya 1)", 
                                             options=range(len(file_ids)),
                                             format_func=lambda i: file_names[i],
                                             key="file1_select")

            # Allow selecting two files for comparison
            selected_file_index2 = st.selectbox("İşleme için bir dosya seçin (Dosya 2)", 
                                             options=range(len(file_ids)),
                                             format_func=lambda i: file_names[i],
                                             key="file2_select")
            col1, col2 = st.columns(2)
            with col1:
                compare_button = st.button("Seçili Dosyaları Karşılaştır")
            with col2:
                save_as_revision = st.checkbox("Revizyon olarak kaydet", value=True)

            if compare_button:
                with st.spinner("📊 Dosyalar karşılaştırılıyor..."):
                    try:
                        file1_id = file_ids[selected_file_index]
                        file2_id = file_ids[selected_file_index2]

                        file1_data = db.get_file_by_id(file1_id)
                        file2_data = db.get_file_by_id(file2_id)

                        if file1_data and file2_data:
                            try:
                                # Directly compare the two Excel files
                                comparison_result = excel_processor.compare_excel_files(
                                    file1_data['filepath'],
                                    file2_data['filepath']
                                )
                                st.session_state.comparison_result = comparison_result
                            except Exception as e:
                                st.error(f"Error comparing files: {str(e)}")

                        # Display comparison results
                        st.success(f"Compared {file1_data['filename']} with {file2_data['filename']}")
                        st.write(f"Found {len(comparison_result)} differences")

                        # Create DataFrame from comparison results
                        if comparison_result:
                            diff_df = pd.DataFrame(comparison_result)
                            st.dataframe(diff_df, use_container_width=True)

                            col1, col2 = st.columns(2)
                            with col1:
                                # Add download button for comparison report
                                report_data = excel_processor.generate_comparison_report(comparison_result)
                                st.download_button(
                                    label="Karşılaştırma Raporunu İndirin",
                                    data=report_data.getvalue(),
                                    file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                            with col2:
                                if st.button("Veri Tabanına Kaydet"):
                                    try:
                                        from migrate_to_supabase import get_supabase_connection
                                        supabase_conn = get_supabase_connection()
                                        if supabase_conn:
                                            save_result = excel_processor.save_to_supabase({
                                                'file1': {'filepath': file1_data['filepath']},
                                                'file2': {'filepath': file2_data['filepath']},
                                                'comparison_data': comparison_result
                                            }, supabase_conn)
                                            if save_result:
                                                st.success("Karşılaştırma sonuçları başarıyla Supabase'e CSV formatında kaydedildi!")
                                                st.info(f"Karşılaştırılan dosyalar:\n- {file1_data['filename']}\n- {file2_data['filename']}")
                                                log_activity("Saved comparison results to Supabase as CSV", db, username)
                                            else:
                                                st.error("Supabase'e kaydetme başarısız oldu")
                                                st.warning("Lütfen bağlantıyı kontrol edin ve tekrar deneyin")

                                    except Exception as e:
                                        st.error(f"Error saving to Supabase: {str(e)}")

                        # Save as revision if checked
                            if save_as_revision:
                                new_revision = file2_data['current_revision'] + 1
                                db.execute("UPDATE files SET current_revision = ? WHERE id = ?", (new_revision, file2_id))
                                db.execute("INSERT INTO file_revisions (file_id, revision_number, revision_path) VALUES (?, ?, ?)",
                                         (file2_id, new_revision, file2_data['filepath']))
                                st.success(f"Saved as revision {new_revision}")

                            log_activity(f"Compared files: {file1_data['filename']} and {file2_data['filename']}", db, username)
                    except Exception as e:
                        st.error(f"Error comparing files: {str(e)}")

    # Comparison tab
    with tab2:
        st.header("Bom Karşılaştırması")

        if st.session_state.selected_file:
            selected_file = st.session_state.selected_file
            st.subheader(f"Selected File: {selected_file['filename']}")

            # Get revisions for this file
            revisions = db.get_file_revisions(selected_file['id'])

            if len(revisions) < 2:
                st.info("Karşılaştırmak için en az iki revizyona ihtiyacınız var. Karşılaştırmayı etkinleştirmek için bu dosyanın yeni bir sürümünü yükleyin.")
            else:
                # Allow user to select revisions to compare
                rev_col1, rev_col2 = st.columns(2)

                with rev_col1:
                    rev1_options = [(r[0], f"Revision {r[2]} - {r[3]}") for r in revisions]
                    rev1_index = st.selectbox("İlk revizyonu seçin", 
                                             options=range(len(rev1_options)),
                                             format_func=lambda i: rev1_options[i][1])
                    rev1_id = rev1_options[rev1_index][0]

                with rev_col2:
                    # Filter out the already selected revision
                    rev2_options = [r for r in rev1_options if r[0] != rev1_id]
                    rev2_index = st.selectbox("İkinci revizyonu seçin", 
                                             options=range(len(rev2_options)),
                                             format_func=lambda i: rev2_options[i][1])
                    rev2_id = rev2_options[rev2_index][0]

                if st.button("Revizyonları Karşılaştır"):
                    rev1_data = db.get_revision_by_id(rev1_id)
                    rev2_data = db.get_revision_by_id(rev2_id)

                    if rev1_data and rev2_data:
                        with st.spinner("Revizyonlar karşılaştırılıyor..."):
                            try:
                                # Get file paths
                                file1_path = rev1_data[4]
                                file2_path = rev2_data[4]

                                # Compare files
                                comparison_result = excel_processor.compare_excel_files(file1_path, file2_path)
                                st.session_state.comparison_result = comparison_result

                                # Log the comparison activity
                                log_activity(f"Karşılaştırılan revizyonlar: {rev1_data[2]} and {rev2_data[2]}", db, username)

                                # Save comparison result to database
                                db.save_comparison_result(
                                    file_id=selected_file['id'],
                                    rev1_id=rev1_id,
                                    rev2_id=rev2_id,
                                    changes_count=len(comparison_result),
                                    comparison_date=datetime.now()
                                )

                                st.success(f"Revizyonlar {len(comparison_result)} arasında farklar bulundu")
                            except Exception as e:
                                st.error(f"Dosyaları karşılaştırırken hata oluştu: {str(e)}")

                # Display comparison results if available
                if st.session_state.comparison_result:
                    st.subheader("Karşılaştırma Sonuçları")

                    if not st.session_state.comparison_result:
                        st.info("Seçilen revizyonlar arasında hiçbir fark bulunamadı")
                    else:
                        # Create DataFrame from comparison results
                        diff_df = pd.DataFrame(st.session_state.comparison_result)

                        # Display differences with enhanced information
                        st.markdown("### 📊 Detaylı Değişiklik Raporu")

                        # Progress bar for changes
                        total_changes = len(diff_df)
                        st.progress(min(total_changes / 100, 1.0), 
                                  text=f"Toplam {total_changes} değişiklik tespit edildi")

                        # Visual metrics
                        metrics_cols = st.columns(4)
                        with metrics_cols[0]:
                            st.metric("💫 Toplam", total_changes)
                        with metrics_cols[1]:
                            st.metric("🔄 Değişiklik", len(diff_df[diff_df['change_type'] == 'modified']))
                        with metrics_cols[2]:
                            st.metric("➕ Eklenen", len(diff_df[diff_df['change_type'] == 'added']))
                        with metrics_cols[3]:
                            st.metric("➖ Silinen", len(diff_df[diff_df['change_type'] == 'removed']))

                        # Show summary statistics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Changes", len(diff_df))
                        with col2:
                            structure_changes = len(diff_df[diff_df['type'] == 'structure'])
                            st.metric("Structure Changes", structure_changes)
                        with col3:
                            cell_changes = len(diff_df[diff_df['type'] == 'cell'])
                            st.metric("Cell Changes", cell_changes)

                        # Format the DataFrame for display
                        diff_df['timestamp'] = pd.to_datetime(diff_df['modified_date'])
                        diff_df['change_description'] = diff_df.apply(
                            lambda x: f"{x['type'].title()} change by {x['modified_by']} at {x['timestamp'].strftime('%Y-%m-%d %H:%M')}", 
                            axis=1
                        )

                        # Display the formatted DataFrame
                        st.dataframe(
                            diff_df[['change_description', 'row', 'column', 'value1', 'value2', 'change_type']],
                            column_config={
                                'change_description': 'Change Details',
                                'value1': 'Original Value',
                                'value2': 'New Value',
                                'change_type': 'Change Type'
                            },
                            use_container_width=True
                        )

                        # Download comparison report as Excel
                        if st.download_button(
                            label="Karşılaştırma Raporunu İndirin",
                            data=excel_processor.generate_comparison_report(st.session_state.comparison_result).getvalue(),
                            file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ):
                            log_activity("Downloaded comparison report", db, username)
                            st.success("Karşılaştırma raporu başarıyla indirildi")
        else:
            st.info("Karşılaştırmayı etkinleştirmek için lütfen Dosyalar sekmesinden bir dosya seçin")

    # Auto-Compare tab (New Feature)
    with tab3:
        st.header("Otomatik Bom Karşılaştırması")
        st.write("Son iki kişinin miktarı otomatik olarak karşılaştırılır.")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Dosyaları Karşılaştır"):
                with st.spinner("Bom dosyaları karşılaştırılıyor..."):
                    try:
                        latest_files = db.get_recent_files(2)
                        if len(latest_files) >= 2:
                            file1_data = latest_files[0]
                            file2_data = latest_files[1]

                            comparison_result = excel_processor.compare_excel_files(
                                file1_data['filepath'],
                                file2_data['filepath']
                            )

                            auto_result = {
                                'file1': file1_data,
                                'file2': file2_data,
                                'comparison_data': comparison_result,
                                'comparison_count': len(comparison_result)
                            }

                            st.session_state.auto_comparison_result = auto_result

                            # Save to database
                            comparison_id = db.save_comparison_result(
                                file2_data['id'],
                                file1_data['id'],
                                file2_data['id'],
                                len(comparison_result),
                                datetime.now(),
                                comparison_result
                            )

                            if comparison_id:
                                st.success(f"Karşılaştırma tamamlandı!")
                                st.info(f"Toplam {len(comparison_result)} fark bulundu:")
                                st.write(f"- Dosya 1: {os.path.basename(file1_data['filepath'])}")
                                st.write(f"- Dosya 2: {os.path.basename(file2_data['filepath'])}")
                        else:
                            st.warning("En az iki dosya gerekli")
                    except Exception as e:
                        st.error(f"Karşılaştırma hatası: {str(e)}")

        with col2:
            if st.button("Son Karşılaştırmaları Göster"):
                try:
                    from migrate_to_supabase import get_supabase_connection
                    supabase_conn = get_supabase_connection()
                    if supabase_conn:
                        cursor = supabase_conn.cursor()
                        cursor.execute("SELECT * FROM comparison_results ORDER BY created_at DESC LIMIT 5")
                        recent_comparisons = cursor.fetchall()
                        if recent_comparisons:
                            st.write("Son Karşılaştırmalar:")
                            for comp in recent_comparisons:
                                st.write(f"- {comp[3]} vs {comp[4]} ({comp[5]} fark)")
                        else:
                            st.info("Henüz kaydedilmiş karşılaştırma bulunmuyor")
                except Exception as e:
                    st.error(f"Karşılaştırma geçmişi yüklenemedi: {str(e)}")

        # Display auto-comparison results if available
        if st.session_state.auto_comparison_result:
            st.subheader("Otomatik Karşılaştırma Sonuçları")

            # Display file info
            st.write(f"First file: **{os.path.basename(st.session_state.auto_comparison_result['file1']['filepath'])}**")
            st.write(f"Second file: **{os.path.basename(st.session_state.auto_comparison_result['file2']['filepath'])}**")
            st.write(f"Found **{st.session_state.auto_comparison_result['comparison_count']}** differences")
            st.write(f"Modified by: **{st.session_state.auto_comparison_result.get('modified_by', 'System')}**")
            st.write(f"Comparison date: **{st.session_state.auto_comparison_result.get('comparison_date', '')}**")

            # Display comparison data in a table
            if 'comparison_data' in st.session_state.auto_comparison_result:
                comparison_df = pd.DataFrame(st.session_state.auto_comparison_result['comparison_data'])
                st.dataframe(comparison_df)

            # Display report download button
            report_file = st.session_state.auto_comparison_result.get('report_file')
            if report_file and os.path.exists(report_file):
                with open(report_file, 'rb') as f:
                    if st.download_button(
                        label="Otomatik Karşılaştırma Raporunu İndirin",
                        data=f.read(),
                        file_name=f"auto_comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ):
                        log_activity(f"Downloaded auto-comparison report: {report_file}", db, username)
                        st.success("Downloaded auto-comparison report successfully")
            else:
                # If no file saved, generate a new one
                if 'comparison_data' in st.session_state.auto_comparison_result:
                    comparison_data = st.session_state.auto_comparison_result['comparison_data']
                    if st.download_button(
                        label="Otomatik Karşılaştırma Raporunu İndirin",
                        data=excel_processor.generate_comparison_report(comparison_data).getvalue(),
                        file_name=f"auto_comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ):
                        log_activity("Downloaded auto-comparison report", db, username)
                        st.success("Otomatik karşılaştırma raporu başarıyla indirildi")

    # History tab
    with tab4:
        st.header("Tarihçe ve Revizyonlar")

        tab4_1, tab4_2 = st.tabs(["Etkinlik Geçmişi", "Revizyon Geçmişi"])

        with tab4_1:
            # Fetch activity logs from database
            activity_logs = db.get_activity_logs()

            if not activity_logs:
                st.info("Henüz etkinlik kaydedilmedi")
            else:
                # Create DataFrame for logs
                logs_df = pd.DataFrame(activity_logs, columns=["ID", "User", "Activity", "Timestamp"])
                st.dataframe(logs_df, use_container_width=True)

        with tab4_2:
            st.subheader("Son BOM Karşılaştırma Raporu")
            
            # Get the latest comparison results
            latest_files = db.get_recent_files(2)
            
            if len(latest_files) >= 2:
                file1 = latest_files[0]
                file2 = latest_files[1]
                
                st.write(f"Karşılaştırılan Dosyalar:")
                st.write(f"1. {file1['filename']}")
                st.write(f"2. {file2['filename']}")
                
                try:
                    # Compare the files
                    comparison_results = excel_processor.compare_excel_files(
                        file1['filepath'],
                        file2['filepath']
                    )
                    
                    if comparison_results:
                        # Display differences count
                        st.info(f"Toplam {len(comparison_results)} değişiklik bulundu")
                        
                        # Create DataFrame from comparison results
                        diff_df = pd.DataFrame([{
                            'Değişiklik Tipi': 'Yapısal Değişiklik' if r['type'] == 'structure' else 'Hücre Değişikliği',
                            'Sütun': r.get('column', ''),
                            'Satır': r.get('row', ''),
                            'Eski Değer': r.get('value1', ''),
                            'Yeni Değer': r.get('value2', ''),
                            'Değişiklik': r.get('change_type', '')
                        } for r in comparison_results])
                        
                        st.dataframe(diff_df)
                        
                        # Generate and offer download of comparison report
                        report_data = excel_processor.generate_comparison_report(comparison_results)
                        st.download_button(
                            "Karşılaştırma Raporunu İndir",
                            report_data.getvalue(),
                            file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.success("Dosyalar arasında fark bulunmadı")
                except Exception as e:
                    st.error(f"Karşılaştırma hatası: {str(e)}")
            else:
                st.warning("Karşılaştırma için en az iki Bom dosyası gerekli")
                comparisons = db.get_comparison_history() 
                if comparisons:
                    st.subheader("Bom Karşılaştırma Revizyonları")
                    comp_data = []
                    for comp in comparisons:
                        rev1 = db.get_revision_by_id(comp[3])
                        rev2 = db.get_revision_by_id(comp[4])
                        comp_data.append({
                            "Karşılaştırma Tarihi": comp[6],
                            "Değişiklik Sayısı": comp[5],
                            "Eski Revizyon": f"Rev {rev1['revision_number'] if rev1 else 'N/A'}",
                            "Yeni Revizyon": f"Rev {rev2['revision_number'] if rev2 else 'N/A'}"
                            })

                        if comp_data:
                            comp_df = pd.DataFrame(comp_data)
                            st.dataframe(comp_df, use_container_width=True)

                            # Karşılaştırma detayları için seçim kutusu
                            selected_comp = st.selectbox(
                                "Karşılaştırma detaylarını görüntüle",
                                range(len(comp_data)),
                                format_func=lambda i: f"{comp_data[i]['Karşılaştırma Tarihi']} ({comp_data[i]['Değişiklik Sayısı']} değişiklik)"
                            )

                            if st.button("Detayları Göster"):
                                comparison = comparisons[selected_comp]
                                # Karşılaştırma raporunu görüntüle
                                report_path = os.path.join('comparison_reports', f"comparison_report_{comparison[6].strftime('%Y%m%d_%H%M%S')}.xlsx")
                                if os.path.exists(report_path):
                                    with open(report_path, 'rb') as f:
                                        st.download_button(
                                            "Karşılaştırma Raporunu İndir",
                                            f,
                                            file_name=f"comparison_report_{comparison[6].strftime('%Y%m%d_%H%M%S')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                        else:
                            st.info("Bu dosya için henüz karşılaştırma revizyonu bulunmuyor")
                else:
                    st.info("Sistemde dosya bulunamadı")

        if not activity_logs:
            st.info("Henüz etkinlik kaydedilmedi")
        else:
            # Create DataFrame for logs
            logs_df = pd.DataFrame(activity_logs, columns=["ID", "User", "Activity", "Timestamp"])

            # Display logs
            st.dataframe(logs_df, use_container_width=True)

            # Simple activity visualization
            if len(logs_df) > 0:
                # Count activities by type
                activity_counts = logs_df["Activity"].apply(
                    lambda x: "Dosya İşlemleri" if "process" in x.lower() else
                              "Karşılaştırma" if "compar" in x.lower() else
                              "İzleme" if "monitor" in x.lower() else
                              "Doğrulama" if "log" in x.lower() else
                              "Dışarı Aktar" if "export" in x.lower() else
                              "Diğer"
                ).value_counts().reset_index()
                activity_counts.columns = ["Activity Type", "Count"]

                # Create pie chart
                fig = px.pie(activity_counts, values="Count", names="Activity Type", 
                             title="Faaliyet Dağılımı")
                st.plotly_chart(fig, use_container_width=True)

    # Export to ERP tab
    with tab5:
        st.header("ERP'ye aktar")

        # First check for auto-comparison results
        if st.session_state.auto_comparison_result:
            st.subheader("Otomatik Karşılaştırma Sonuçlarını ERP'ye Aktar")
            # Add auto-comparison export UI and functionality here
        elif st.session_state.comparison_result:
            st.subheader("Karşılaştırma Sonuçlarını ERP'ye Aktar")
        else:
            st.info("Lütfen ERP ihracatını etkinleştirmek için önce bir karşılaştırma çalıştırın")

        if st.session_state.auto_comparison_result or st.session_state.comparison_result:
            # Connection settings
            with st.expander("ERP Bağlantı Ayarları"):
                erp_host = st.text_input("ERP Host", value="localhost")
                erp_port = st.number_input("ERP Port", value=5432)
                erp_db =st.text_input("ERP Database", value="erp_database")
                erp_user = st.text_input("ERP Username", value="erp_user")
                erp_password = st.text_input("ERP Password", type="password")

            # Export options
            export_format = st.selectbox("Export Format", ["JSON", "CSV", "Direct DB Connection"])
            include_metadata = st.checkbox("Include File Metadata", value=True)

            if st.button("ERP'ye aktar"):
                with st.spinner("Exporting data to ERP..."):
                    try:
                        # Create connection parameters
                        connection_params = {
                            "host": erp_host,
                            "port": erp_port,
                            "database": erp_db,
                            "user": erp_user,
                            "password": erp_password,
                            "format": export_format.lower()
                        }

                        # Determine which comparison result to use
                        export_data = st.session_state.auto_comparison_result.get('comparison_data') if st.session_state.auto_comparison_result else st.session_state.comparison_result
                        file_info = None

                        if include_metadata:
                            if st.session_state.auto_comparison_result:
                                file_info = {
                                    'filename': os.path.basename(st.session_state.auto_comparison_result['file2']['filepath']),
                                    'filepath': st.session_state.auto_comparison_result['file2']['filepath']
                                }
                            else:
                                file_info = st.session_state.selected_file

                        # Export the data
                        export_result = erp_exporter.export_to_erp(
                            export_data,
                            connection_params,
                            include_metadata,
                            file_info=file_info
                        )

                        # Log the export activity
                        log_activity(f"Exported comparison data to ERP in {export_format} format", db, username)

                        st.success(f"Veriler başarıyla dışa aktarıldı: {export_result}")
                    except Exception as e:
                        st.error(f"ERP'ye aktarmada hata: {str(e)}")
                        log_activity(f"ERP dışa aktarımı başarısız oldu: {str(e)}", db, username)

        # Manual data export
        st.subheader("Manuel Veri Dışa Aktarımı")

        # Get all files
        files = db.get_all_files()

        if not files:
            st.info("Dışa aktarma için dosya yok")
        else:
            # Select file to export
            file_ids = [f[0] for f in files]
            file_names = [f[1] for f in files]
            selected_export_file = st.selectbox("Dışa aktarılacak dosyayı seçin", 
                                           options=range(len(file_ids)),
                                           format_func=lambda i: file_names[i])

            export_selected_file_id = file_ids[selected_export_file]

            if st.button("ERP İhracatı için Dosyayı Hazırla"):
                file_data = db.get_file_by_id(export_selected_file_id)

                if file_data:
                    try:
                        # Process the file for export
                        export_data = excel_processor.prepare_for_export(file_data['filepath'])

                        # Show export data preview
                        st.subheader("Veri Önizlemesini Dışa Aktar")
                        st.json(export_data)

                        # Save as JSON for download
                        if st.download_button(
                            label="Dışa Aktarma Verilerini İndir",
                            data=erp_exporter.generate_export_file(export_data, "json"),
                            file_name=f"erp_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        ):
                            log_activity(f"Downloaded ERP export data for file: {file_data['filename']}", db, username)
                            st.success("İhracat verileri başarıyla indirildi")
                    except Exception as e:
                        st.error(f"Error preparing file for export: {str(e)}")
else:
    st.warning("Sisteme erişmek için lütfen giriş yapın")
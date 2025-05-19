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
    page_title="WSCAD Excel Comparison System",
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
    st.success("WSCAD Excel karşılaştırma sistemi hazır.")

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
        st.error(f"Monitoring start error: {e}")
        return False

def stop_monitoring():
    """Stop the file monitoring thread"""
    global file_monitor
    if file_monitor:
        file_monitor.stop_monitoring()
        st.session_state.monitoring = False
        log_activity("Stopped directory monitoring", db, username)

def auto_compare_latest_files(directory='.'):
    """Find and compare the two most recent Excel files"""
    try:
        # Use the Excel processor to find and compare latest files
        comparison_result = excel_processor.auto_compare_latest_files(directory)

        # Log the activity
        log_activity(f"Auto-compared latest files: {os.path.basename(comparison_result['file1']['filepath'])} and {os.path.basename(comparison_result['file2']['filepath'])}", db, username)

        return comparison_result
    except Exception as e:
        st.error(f"Auto-comparison error: {str(e)}")
        log_activity(f"Auto-comparison failed: {str(e)}", db, username)
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
    st.title("WSCAD Excel Comparison and Process Tracking System")

    # Help/Guide section
    with st.sidebar:
        with st.expander("📚 Nasıl Kullanılır?"):
            st.markdown("""
            ### Hızlı Başlangıç Rehberi
            1. **Dosya Seçimi**: 'Files' sekmesinden Excel dosyalarınızı seçin
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
        st.header(f"Welcome, {username}")

        # Directory monitoring section
        st.subheader("Dizin İzleme")

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
            directory = st.text_input("Özel dizin yolu girin", help="Excel dosyalarının bulunduğu dizini girin")
        else:
            directory = default_dirs[selected_dir_option]
            st.text(f"Seçilen dizin: {directory}")

        # Directory validation
        is_valid_dir = os.path.exists(directory) if directory else False

        if is_valid_dir:
            st.success(f"✅ Geçerli dizin: {directory}")
            excel_files = excel_processor.list_excel_files(directory)
            if excel_files:
                st.info(f"📊 Bu dizinde {len(excel_files)} Excel dosyası bulundu")
                for file in excel_files[:5]:  # Son 5 dosyayı göster
                    st.text(f"📑 {file['filename']} - {file['modified']}")

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
            if recent_files:
                st.subheader("Son Dosya Aktiviteleri")
                for file in recent_files:
                    st.text(f"📄 {file[1]} - {file[4]}")
        else:
            st.warning("Dizin izleme aktif değil")

        # Auto-comparison button (New Feature)
        st.subheader("Quick Auto-Compare")
        if st.button("Compare Latest Excel Files"):
            with st.spinner("Finding and comparing the latest Excel files..."):
                try:
                    auto_result = auto_compare_latest_files()
                    if auto_result:
                        st.session_state.auto_comparison_result = auto_result
                        st.success(f"Otomatik karşılaştırma tamamlandı! {auto_result['comparison_count']} fark bulundu")
                except Exception as e:
                    st.error(f"Otomatik karşılaştırma hatası: {str(e)}")

        # Logout button
        if st.button("Logout"):
            log_activity("User logged out", db, username)
            st.session_state.clear()
            st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Files", "Comparison", "Auto-Compare", "History", "Export to ERP"])

    # Files tab
    with tab1:
        st.header("Excel Files")

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
                st.info("No files have been detected yet. Start monitoring a directory to detect WSCAD Excel files.")
            else:
                st.info("Monitoring is stopped. Start monitoring to detect Excel files.")
        else:
            # Create a DataFrame for display
            files_df = pd.DataFrame(files, columns=["ID", "Filename", "Path", "Size (KB)", "Detected Time", "Processed", "Current Revision"])

            # Display the files
            st.dataframe(files_df, use_container_width=True)

            # Allow user to select a file
            file_ids = [f[0] for f in files]
            file_names = [f[1] for f in files]
            selected_file_index = st.selectbox("Select a file for processing", 
                                              options=range(len(file_ids)),
                                              format_func=lambda i: file_names[i])

            # Allow selecting two files for comparison
            selected_file_index2 = st.selectbox("Select second file for comparison", 
                                             options=range(len(file_ids)),
                                             format_func=lambda i: file_names[i])

            col1, col2 = st.columns(2)
            with col1:
                compare_button = st.button("Compare Selected Files")
            with col2:
                save_as_revision = st.checkbox("Save as Revision", value=True)

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
                                    label="Download Comparison Report",
                                    data=report_data.getvalue(),
                                    file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                            with col2:
                                if st.button("Save to Supabase"):
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
        st.header("Excel Comparison")

        if st.session_state.selected_file:
            selected_file = st.session_state.selected_file
            st.subheader(f"Selected File: {selected_file['filename']}")

            # Get revisions for this file
            revisions = db.get_file_revisions(selected_file['id'])

            if len(revisions) < 2:
                st.info("Need at least two revisions to compare. Upload a new version of this file to enable comparison.")
            else:
                # Allow user to select revisions to compare
                rev_col1, rev_col2 = st.columns(2)

                with rev_col1:
                    rev1_options = [(r[0], f"Revision {r[2]} - {r[3]}") for r in revisions]
                    rev1_index = st.selectbox("Select first revision", 
                                             options=range(len(rev1_options)),
                                             format_func=lambda i: rev1_options[i][1])
                    rev1_id = rev1_options[rev1_index][0]

                with rev_col2:
                    # Filter out the already selected revision
                    rev2_options = [r for r in rev1_options if r[0] != rev1_id]
                    rev2_index = st.selectbox("Select second revision", 
                                             options=range(len(rev2_options)),
                                             format_func=lambda i: rev2_options[i][1])
                    rev2_id = rev2_options[rev2_index][0]

                if st.button("Compare Revisions"):
                    rev1_data = db.get_revision_by_id(rev1_id)
                    rev2_data = db.get_revision_by_id(rev2_id)

                    if rev1_data and rev2_data:
                        with st.spinner("Comparing revisions..."):
                            try:
                                # Get file paths
                                file1_path = rev1_data[4]
                                file2_path = rev2_data[4]

                                # Compare files
                                comparison_result = excel_processor.compare_excel_files(file1_path, file2_path)
                                st.session_state.comparison_result = comparison_result

                                # Log the comparison activity
                                log_activity(f"Compared revisions: {rev1_data[2]} and {rev2_data[2]}", db, username)

                                # Save comparison result to database
                                db.save_comparison_result(
                                    file_id=selected_file['id'],
                                    rev1_id=rev1_id,
                                    rev2_id=rev2_id,
                                    changes_count=len(comparison_result),
                                    comparison_date=datetime.now()
                                )

                                st.success(f"Found {len(comparison_result)} differences between revisions")
                            except Exception as e:
                                st.error(f"Error comparing files: {str(e)}")

                # Display comparison results if available
                if st.session_state.comparison_result:
                    st.subheader("Comparison Results")

                    if not st.session_state.comparison_result:
                        st.info("No differences found between the selected revisions")
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
                            label="Download Comparison Report",
                            data=excel_processor.generate_comparison_report(st.session_state.comparison_result).getvalue(),
                            file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ):
                            log_activity("Downloaded comparison report", db, username)
                            st.success("Downloaded comparison report successfully")
        else:
            st.info("Please select a file from the Files tab to enable comparison")

    # Auto-Compare tab (New Feature)
    with tab3:
        st.header("Auto Excel Comparison")
        st.write("Son eklenen iki Excel dosyasını otomatik olarak karşılaştırır.")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Dosyaları Karşılaştır"):
                with st.spinner("Excel dosyaları karşılaştırılıyor..."):
                    try:
                        auto_result = auto_compare_latest_files()
                        if auto_result:
                            st.session_state.auto_comparison_result = auto_result
                            st.success(f"Karşılaştırma tamamlandı! {auto_result['comparison_count']} fark bulundu.")

                            # Supabase'e kaydetme seçeneği
                            try:
                                from migrate_to_supabase import get_supabase_connection
                                supabase_conn = get_supabase_connection()
                                if supabase_conn:
                                    save_result = excel_processor.save_to_supabase({
                                        'file1': auto_result['file1'],
                                        'file2': auto_result['file2'],
                                        'comparison_data': auto_result.get('comparison_data', [])
                                    }, supabase_conn)
                                    if save_result:
                                        st.success("Sonuçlar Supabase'e kaydedildi")
                                        log_activity("Auto-comparison results saved to Supabase", db, username)
                            except Exception as e:
                                st.warning(f"Supabase'e kaydetme başarısız: {str(e)}")
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
            st.subheader("Auto-Comparison Results")

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
                        label="Download Auto-Comparison Report",
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
                        label="Download Auto-Comparison Report",
                        data=excel_processor.generate_comparison_report(comparison_data).getvalue(),
                        file_name=f"auto_comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ):
                        log_activity("Downloaded auto-comparison report", db, username)
                        st.success("Downloaded auto-comparison report successfully")

    # History tab
    with tab4:
        st.header("History & Revisions")

        tab4_1, tab4_2 = st.tabs(["Activity History", "Revision History"])

        with tab4_1:
            # Fetch activity logs from database
            activity_logs = db.get_activity_logs()

            if not activity_logs:
                st.info("No activity recorded yet")
            else:
                # Create DataFrame for logs
                logs_df = pd.DataFrame(activity_logs, columns=["ID", "User", "Activity", "Timestamp"])
                st.dataframe(logs_df, use_container_width=True)

        with tab4_2:
            # Get all files for selection
            files = db.get_all_files()
            if files:
                file_options = {f[1]: f[0] for f in files}  # filename: id mapping
                selected_file = st.selectbox("Select File", options=list(file_options.keys()))

                if selected_file:
                    file_id = file_options[selected_file]
                    # Get comparison history for this file
                    comparisons = db.get_comparison_history(file_id)

                    if comparisons:
                        st.subheader("Excel Karşılaştırma Revizyonları")
                        comp_data = []
                        for comp in comparisons:
                            comp_data.append({
                                "Karşılaştırma Tarihi": comp[6],
                                "Değişiklik Sayısı": comp[5],
                                "Eski Revizyon": f"Rev {db.get_revision_by_id(comp[3])[2]}",
                                "Yeni Revizyon": f"Rev {db.get_revision_by_id(comp[4])[2]}"
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
                st.info("No files found in the system")

        if not activity_logs:
            st.info("No activity recorded yet")
        else:
            # Create DataFrame for logs
            logs_df = pd.DataFrame(activity_logs, columns=["ID", "User", "Activity", "Timestamp"])

            # Display logs
            st.dataframe(logs_df, use_container_width=True)

            # Simple activity visualization
            if len(logs_df) > 0:
                # Count activities by type
                activity_counts = logs_df["Activity"].apply(
                    lambda x: "File Processing" if "process" in x.lower() else
                              "Comparison" if "compar" in x.lower() else
                              "Monitoring" if "monitor" in x.lower() else
                              "Authentication" if "log" in x.lower() else
                              "Export" if "export" in x.lower() else
                              "Other"
                ).value_counts().reset_index()
                activity_counts.columns = ["Activity Type", "Count"]

                # Create pie chart
                fig = px.pie(activity_counts, values="Count", names="Activity Type", 
                             title="Activity Distribution")
                st.plotly_chart(fig, use_container_width=True)

    # Export to ERP tab
    with tab5:
        st.header("Export to ERP")

        # First check for auto-comparison results
        if st.session_state.auto_comparison_result:
            st.subheader("Export Auto-Comparison Results to ERP")
            # Add auto-comparison export UI and functionality here
        elif st.session_state.comparison_result:
            st.subheader("Export Comparison Results to ERP")
        else:
            st.info("Please run a comparison first to enable ERP export")

        if st.session_state.auto_comparison_result or st.session_state.comparison_result:
            # Connection settings
            with st.expander("ERP Connection Settings"):
                erp_host = st.text_input("ERP Host", value="localhost")
                erp_port = st.number_input("ERP Port", value=5432)
                erp_db = st.text_input("ERP Database", value="erp_database")
                erp_user = st.text_input("ERP Username", value="erp_user")
                erp_password = st.text_input("ERP Password", type="password")

            # Export options
            export_format = st.selectbox("Export Format", ["JSON", "CSV", "Direct DB Connection"])
            include_metadata = st.checkbox("Include File Metadata", value=True)

            if st.button("Export to ERP"):
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

                        st.success(f"Data exported successfully: {export_result}")
                    except Exception as e:
                        st.error(f"Error exporting to ERP: {str(e)}")
                        log_activity(f"ERP export failed: {str(e)}", db, username)

        # Manual data export
        st.subheader("Manual Data Export")

        # Get all files
        files = db.get_all_files()

        if not files:
            st.info("No files available for export")
        else:
            # Select file to export
            file_ids = [f[0] for f in files]
            file_names = [f[1] for f in files]
            selected_export_file = st.selectbox("Select file to export", 
                                           options=range(len(file_ids)),
                                           format_func=lambda i: file_names[i])

            export_selected_file_id = file_ids[selected_export_file]

            if st.button("Prepare File for ERP Export"):
                file_data = db.get_file_by_id(export_selected_file_id)

                if file_data:
                    try:
                        # Process the file for export
                        export_data = excel_processor.prepare_forexport(file_data['filepath'])

                        # Show export data preview
                        st.subheader("Export Data Preview")
                        st.json(export_data)

                        # Save as JSON for download
                        if st.download_button(
                            label="Download Export Data",
                            data=erp_exporter.generate_export_file(export_data, "json"),
                            file_name=f"erp_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        ):
                            log_activity(f"Downloaded ERP export data for file: {file_data['filename']}", db, username)
                            st.success("Downloaded export data successfully")
                    except Exception as e:
                        st.error(f"Error preparing file for export: {str(e)}")
else:
    st.warning("Please log in to access the system")
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import glob

# Import custom modules
from auth import authenticate
from database import Database
from excel_processor import ExcelProcessor
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

def scan_xlsx_files(directory):
    """Belirtilen dizindeki .xlsx dosyalarını tarar"""
    try:
        if not os.path.exists(directory):
            return []
        
        # .xlsx dosyalarını bul
        xlsx_pattern = os.path.join(directory, "*.xlsx")
        xlsx_files = glob.glob(xlsx_pattern)
        
        # Alt dizinleri de tara
        subdirs_pattern = os.path.join(directory, "**", "*.xlsx")
        xlsx_files.extend(glob.glob(subdirs_pattern, recursive=True))
        
        # Dosya bilgilerini topla
        file_info = []
        for filepath in xlsx_files:
            try:
                stat = os.stat(filepath)
                file_info.append({
                    'filepath': filepath,
                    'filename': os.path.basename(filepath),
                    'size_kb': round(stat.st_size / 1024, 2),
                    'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })
            except:
                continue
        
        # Değiştirilme tarihine göre sırala (en yeni önce)
        file_info.sort(key=lambda x: x['modified'], reverse=True)
        return file_info
        
    except Exception as e:
        st.error(f"Dosya tarama hatası: {e}")
        return []

# Main application logic
if auth_status:
    # Initialize session state
    if 'selected_files' not in st.session_state:
        st.session_state.selected_files = []
    
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
            1. **Dizin Seçimi**: İzlemek istediğiniz dizini seçin
            2. **Dosya Tarama**: .xlsx dosyalarını otomatik olarak bulur
            3. **Karşılaştırma**: İki dosyayı seçip karşılaştırın
            4. **Sonuçlar**: Değişiklikleri detaylı raporlarda görüntüleyin
            """)

    # Sidebar
    with st.sidebar:
        st.header(f"Hoşlgeldiniz, {username}")

        # Directory selection section
        st.subheader("Dizin Seçimi")

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

        # Directory validation and file scanning
        if directory and os.path.exists(directory):
            st.success(f"✅ Geçerli dizin: {directory}")
            
            if st.button("📂 Dosyaları Tara"):
                with st.spinner("Excel dosyaları taranıyor..."):
                    xlsx_files = scan_xlsx_files(directory)
                    st.session_state.xlsx_files = xlsx_files
                    if xlsx_files:
                        st.success(f"📊 {len(xlsx_files)} adet .xlsx dosyası bulundu")
                        log_activity(f"Scanned directory: {directory}, found {len(xlsx_files)} files", db, username)
                    else:
                        st.warning("Bu dizinde .xlsx dosyası bulunamadı")
        else:
            st.error("❌ Geçersiz dizin yolu")

        # Show found files
        if 'xlsx_files' in st.session_state and st.session_state.xlsx_files:
            st.subheader("Bulunan Dosyalar")
            for i, file in enumerate(st.session_state.xlsx_files[:5]):  # İlk 5 dosyayı göster
                st.text(f"📄 {file['filename']}")
                st.caption(f"   {file['modified']} - {file['size_kb']} KB")

        # Logout button
        if st.button("Çıkış"):
            log_activity("Kullanıcı Çıkış yaptı", db, username)
            st.session_state.clear()
            st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Dosyalar", "Karşılaştırma", "Otomatik Karşılaştırma", "Tarih", "ERP'ye aktar"])

     # Files tab
    with tab1:
        st.header("Excel Dosyaları")

        if 'xlsx_files' not in st.session_state or not st.session_state.xlsx_files:
            st.info("Dosyaları görmek için yan panelden bir dizin seçin ve 'Dosyaları Tara' butonuna tıklayın.")
        else:
            # Display files in organized way
            st.subheader(f"📁 Bulunan Excel Dosyaları ({len(st.session_state.xlsx_files)} adet)")

            # Create DataFrame from files
            files_df = pd.DataFrame(st.session_state.xlsx_files)
            
            # Display files with selection checkboxes
            selected_files = []
            
            for idx, file in enumerate(st.session_state.xlsx_files):
                col1, col2, col3, col4 = st.columns([0.5, 3, 1.5, 1])
                
                with col1:
                    if st.checkbox("", key=f"select_{idx}"):
                        selected_files.append(file)
                
                with col2:
                    st.text(f"📄 {file['filename']}")
                
                with col3:
                    st.caption(f"{file['modified']}")
                
                with col4:
                    st.caption(f"{file['size_kb']} KB")

            st.session_state.selected_files = selected_files

            # Show selected files count
            if selected_files:
                st.info(f"Seçili dosya sayısı: {len(selected_files)}")
                
                # Enable comparison if exactly 2 files selected
                if len(selected_files) == 2:
                    st.success("✅ Karşılaştırma için 2 dosya seçildi")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 Seçili Dosyaları Karşılaştır"):
                            with st.spinner("📊 Dosyalar karşılaştırılıyor..."):
                                try:
                                    file1 = selected_files[0]
                                    file2 = selected_files[1]
                                    
                                    comparison_result = excel_processor.compare_excel_files(
                                        file1['filepath'],
                                        file2['filepath']
                                    )
                                    
                                    st.session_state.comparison_result = comparison_result
                                    
                                    st.success(f"Karşılaştırma tamamlandı!")
                                    st.info(f"Toplam {len(comparison_result)} fark bulundu")
                                    st.write(f"📄 Dosya 1: {file1['filename']}")
                                    st.write(f"📄 Dosya 2: {file2['filename']}")
                                    
                                    log_activity(f"Compared files: {file1['filename']} and {file2['filename']}", db, username)
                                    
                                except Exception as e:
                                    st.error(f"Karşılaştırma hatası: {str(e)}")
                    
                    with col2:
                        if st.session_state.comparison_result:
                            # Download comparison report
                            report_data = excel_processor.generate_comparison_report(st.session_state.comparison_result)
                            st.download_button(
                                label="📥 Karşılaştırma Raporunu İndirin",
                                data=report_data.getvalue(),
                                file_name=f"comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                elif len(selected_files) > 2:
                    st.warning("⚠️ Karşılaştırma için sadece 2 dosya seçin")
                else:
                    st.info("ℹ️ Karşılaştırma için 2 dosya seçin")

            # Display comparison results if available
            if st.session_state.comparison_result:
                st.subheader("📊 Karşılaştırma Sonuçları")
                
                if not st.session_state.comparison_result:
                    st.success("✅ Dosyalar arasında fark bulunamadı")
                else:
                    # Create DataFrame from comparison results
                    diff_df = pd.DataFrame(st.session_state.comparison_result)
                    
                    # Show summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Toplam Değişiklik", len(diff_df))
                    with col2:
                        modified_count = len(diff_df[diff_df['change_type'] == 'modified']) if 'change_type' in diff_df.columns else 0
                        st.metric("🔄 Değişen", modified_count)
                    with col3:
                        added_count = len(diff_df[diff_df['change_type'] == 'added']) if 'change_type' in diff_df.columns else 0
                        st.metric("➕ Eklenen", added_count)
                    with col4:
                        removed_count = len(diff_df[diff_df['change_type'] == 'removed']) if 'change_type' in diff_df.columns else 0
                        st.metric("➖ Silinen", removed_count)
                    
                    # Display the differences table
                    st.dataframe(diff_df, use_container_width=True)

    # Auto-Compare tab (Simplified)
    with tab3:
        st.header("Otomatik Karşılaştırma")
        st.write("En son değiştirilen iki dosyayı otomatik olarak karşılaştırır.")

        if 'xlsx_files' not in st.session_state or len(st.session_state.xlsx_files) < 2:
            st.warning("⚠️ Otomatik karşılaştırma için en az 2 Excel dosyası gerekli")
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**En son dosyalar:**")
                st.write(f"1️⃣ {st.session_state.xlsx_files[0]['filename']}")
                st.write(f"2️⃣ {st.session_state.xlsx_files[1]['filename']}")
            
            with col2:
                if st.button("🚀 Otomatik Karşılaştır"):
                    with st.spinner("En son dosyalar karşılaştırılıyor..."):
                        try:
                            file1 = st.session_state.xlsx_files[0]
                            file2 = st.session_state.xlsx_files[1]
                            
                            comparison_result = excel_processor.compare_excel_files(
                                file1['filepath'],
                                file2['filepath']
                            )
                            
                            auto_result = {
                                'file1': file1,
                                'file2': file2,
                                'comparison_data': comparison_result,
                                'comparison_count': len(comparison_result)
                            }
                            
                            st.session_state.auto_comparison_result = auto_result
                            
                            st.success(f"✅ Otomatik karşılaştırma tamamlandı!")
                            st.info(f"📊 Toplam {len(comparison_result)} fark bulundu")
                            
                            log_activity(f"Auto-compared: {file1['filename']} and {file2['filename']}", db, username)
                            
                        except Exception as e:
                            st.error(f"❌ Otomatik karşılaştırma hatası: {str(e)}")

            # Display auto-comparison results
            if st.session_state.auto_comparison_result:
                st.subheader("🎯 Otomatik Karşılaştırma Sonuçları")
                
                result = st.session_state.auto_comparison_result
                st.write(f"📄 **Dosya 1:** {result['file1']['filename']}")
                st.write(f"📄 **Dosya 2:** {result['file2']['filename']}")
                st.write(f"📊 **Fark Sayısı:** {result['comparison_count']}")
                
                if result['comparison_data']:
                    # Display comparison data
                    comparison_df = pd.DataFrame(result['comparison_data'])
                    st.dataframe(comparison_df, use_container_width=True)
                    
                    # Download button for auto-comparison
                    report_data = excel_processor.generate_comparison_report(result['comparison_data'])
                    st.download_button(
                        label="📥 Otomatik Karşılaştırma Raporunu İndirin",
                        data=report_data.getvalue(),
                        file_name=f"auto_comparison_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

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
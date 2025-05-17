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
    """Start monitoring the specified directory in a separate thread"""
    global file_monitor, file_monitor_thread
    
    # Create file monitor if not already created
    if file_monitor is None:
        file_monitor = FileMonitor(directory, db)
    
    # Start monitoring in a thread if not already running
    if file_monitor_thread is None or not file_monitor_thread.is_alive():
        file_monitor_thread = threading.Thread(
            target=file_monitor.start_monitoring,
            daemon=True
        )
        file_monitor_thread.start()
        st.session_state.monitoring = True
        log_activity(f"Started monitoring directory: {directory}", db, username)

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
    
    # Sidebar
    with st.sidebar:
        st.header(f"Welcome, {username}")
        
        # Directory monitoring section
        st.subheader("Directory Monitoring")
        directory = st.text_input("Directory to monitor", value=os.path.expanduser("~/Downloads"))
        
        col1, col2 = st.columns(2)
        with col1:
            if not st.session_state.monitoring:
                if st.button("Start Monitoring"):
                    start_monitoring(directory)
        with col2:
            if st.session_state.monitoring:
                if st.button("Stop Monitoring"):
                    stop_monitoring()
        
        # Show monitoring status
        if st.session_state.monitoring:
            st.success("Directory monitoring is active")
        else:
            st.warning("Monitoring is inactive")

        # Auto-comparison button (New Feature)
        st.subheader("Quick Auto-Compare")
        if st.button("Compare Latest Excel Files"):
            with st.spinner("Finding and comparing the latest Excel files..."):
                auto_result = auto_compare_latest_files()
                if auto_result:
                    st.session_state.auto_comparison_result = auto_result
                    st.success(f"Auto-comparison completed! Found {auto_result['comparison_count']} differences")
        
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
        
        # Fetch all files from the database
        files = db.get_all_files()
        
        if not files:
            st.info("No files have been detected yet. Start monitoring a directory to detect WSCAD Excel files.")
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
            
            if st.button("Load Selected File"):
                selected_file_id = file_ids[selected_file_index]
                file_data = db.get_file_by_id(selected_file_id)
                
                if file_data:
                    st.session_state.selected_file = file_data
                    log_activity(f"Loaded file: {file_data['filename']}", db, username)
                    st.success(f"Loaded file: {file_data['filename']}")
                    
                    # Process file if not already processed
                    if not file_data['processed']:   # Check processed flag
                        try:
                            excel_processor.process_file(file_data['filepath']) # Process file at path
                            db.mark_file_as_processed(selected_file_id)
                            log_activity(f"Processed file: {file_data['filename']}", db, username)
                            st.success("File processed successfully")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error processing file: {str(e)}")
    
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
                        
                        # Display differences
                        st.dataframe(diff_df, use_container_width=True)
                        
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
        st.write("This tab automatically compares the latest two Excel files found in the monitored directory.")
        
        if st.button("Find and Compare Latest Files"):
            with st.spinner("Finding and comparing the latest Excel files..."):
                auto_result = auto_compare_latest_files()
                if auto_result:
                    st.session_state.auto_comparison_result = auto_result
                    st.success(f"Auto-comparison completed!")
        
        # Display auto-comparison results if available
        if st.session_state.auto_comparison_result:
            st.subheader("Auto-Comparison Results")
            
            # Display file info
            st.write(f"First file: **{os.path.basename(st.session_state.auto_comparison_result['file1']['filepath'])}**")
            st.write(f"Second file: **{os.path.basename(st.session_state.auto_comparison_result['file2']['filepath'])}**")
            st.write(f"Found **{st.session_state.auto_comparison_result['comparison_count']}** differences")
            
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
        st.header("Activity History")
        
        # Fetch activity logs from database
        activity_logs = db.get_activity_logs()
        
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
                        export_data = excel_processor.prepare_for_export(file_data['filepath'])
                        
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

    
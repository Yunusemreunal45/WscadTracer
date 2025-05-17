import json
import csv
import io
import sqlite3
import os
from datetime import datetime
import pandas as pd

class ERPExporter:
    """Class for exporting data to ERP systems or other databases"""
    
    def __init__(self):
        """Initialize the ERP exporter"""
        pass
    
    def export_to_erp(self, data, connection_params, include_metadata=True, file_info=None):
        """Export data to an ERP system or database"""
        try:
            export_format = connection_params.get('format', 'json')
            
            if export_format == 'json':
                return self.export_as_json(data, connection_params, include_metadata, file_info)
            elif export_format == 'csv':
                return self.export_as_csv(data, connection_params, include_metadata, file_info)
            elif export_format == 'direct db connection':
                return self.export_to_database(data, connection_params, include_metadata, file_info)
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
        except Exception as e:
            raise Exception(f"Export failed: {str(e)}")
    
    def export_as_json(self, data, connection_params, include_metadata=True, file_info=None):
        """Export data as a JSON file"""
        try:
            # Prepare the export data structure
            export_data = self._prepare_export_data(data, include_metadata, file_info)
            
            # Format the JSON data
            json_data = json.dumps(export_data, indent=2, default=str)
            
            # If connection parameters include a path, write to file
            output_path = connection_params.get('output_path')
            if output_path:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(json_data)
                return f"Data exported to JSON file: {output_path}"
            else:
                # Return success message with data count
                return f"JSON data prepared successfully ({len(data)} records)"
        except Exception as e:
            raise Exception(f"JSON export error: {str(e)}")
    
    def export_as_csv(self, data, connection_params, include_metadata=True, file_info=None):
        """Export data as a CSV file"""
        try:
            # Extract just the cell changes (structure changes don't fit well in CSV)
            cell_changes = [d for d in data if d.get('type') == 'cell']
            
            # Create a DataFrame from the changes
            df = pd.DataFrame(cell_changes)
            
            # If connection parameters include a path, write to file
            output_path = connection_params.get('output_path')
            if output_path:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                df.to_csv(output_path, index=False)
                return f"Data exported to CSV file: {output_path}"
            else:
                # Return CSV as string
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()
                return f"CSV data prepared successfully ({len(cell_changes)} records)"
        except Exception as e:
            raise Exception(f"CSV export error: {str(e)}")
    
    def export_to_database(self, data, connection_params, include_metadata=True, file_info=None):
        """Export data directly to a database"""
        try:
            # Extract connection parameters
            database_url = os.getenv("DATABASE_URL")
            table_name = connection_params.get('table_name', 'excel_changes')
            
            # Use the provided connection parameters or environment variable
            if not database_url:
                # If DATABASE_URL is not set, use the provided connection parameters
                host = connection_params.get('host', 'localhost')
                port = connection_params.get('port', 5432)
                database = connection_params.get('database', '')
                user = connection_params.get('user', '')
                password = connection_params.get('password', '')
                
                database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
            # Connect to the database using SQLAlchemy
            from sqlalchemy import create_engine, text
            
            engine = create_engine(database_url)
            conn = engine.connect()
            
            # Create table if it doesn't exist
            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                file_id INTEGER,
                row_num INTEGER,
                column_name TEXT,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT,
                exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
            conn.execute(text(create_table_sql))
            
            # Insert data
            records_inserted = 0
            for item in data:
                if item.get('type') == 'cell':
                    insert_sql = f'''
                    INSERT INTO {table_name} (file_id, row_num, column_name, old_value, new_value, change_type) 
                    VALUES (:file_id, :row, :column, :old_value, :new_value, :change_type)
                    '''
                    
                    conn.execute(text(insert_sql), {
                        "file_id": file_info.get('id') if file_info else None,
                        "row": item.get('row'),
                        "column": item.get('column'),
                        "old_value": item.get('value1'),
                        "new_value": item.get('value2'),
                        "change_type": item.get('change_type')
                    })
                    
                    records_inserted += 1
            
            # Close connection
            conn.close()
            engine.dispose()
            
            return f"Successfully exported {records_inserted} records to database"
        except Exception as e:
            print(f"Database export error: {str(e)}")
            raise Exception(f"Database export failed: {str(e)}")
    
    def _prepare_export_data(self, data, include_metadata=True, file_info=None):
        """Prepare data structure for export"""
        export_data = {
            'data': data,
            'export_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if include_metadata and file_info:
            export_data['metadata'] = {
                'file_id': file_info.get('id'),
                'filename': file_info.get('filename'),
                'filepath': file_info.get('filepath'),
                'filesize': file_info.get('filesize'),
                'detected_time': file_info.get('detected_time'),
                'current_revision': file_info.get('current_revision')
            }
        
        return export_data
    
    def generate_export_file(self, data, format='json'):
        """Generate an export file in the specified format"""
        try:
            if format.lower() == 'json':
                # Return JSON data as string
                return json.dumps(data, indent=2, default=str)
            elif format.lower() == 'csv':
                # Convert data to CSV
                if 'data' in data and isinstance(data['data'], list):
                    df = pd.DataFrame(data['data'])
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    return csv_buffer.getvalue()
                else:
                    raise ValueError("Data structure not suitable for CSV export")
            else:
                raise ValueError(f"Unsupported export format: {format}")
        except Exception as e:
            raise Exception(f"Error generating export file: {str(e)}")

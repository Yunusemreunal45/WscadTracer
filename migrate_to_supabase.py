import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_supabase_connection():
    """Get connection to Supabase database"""
    try:
        connection = psycopg2.connect(
            host=os.getenv('SUPABASE_HOST'),
            database=os.getenv('SUPABASE_DATABASE'),
            user=os.getenv('SUPABASE_USER'),
            password=os.getenv('SUPABASE_PASSWORD')
        )
        return connection
    except Exception as e:
        print(f"Supabase bağlantı hatası: {e}")
        return None

def setup_supabase_tables(conn):
    """Set up necessary tables in Supabase"""
    try:
        cursor = conn.cursor()

        # Create files table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            filesize INTEGER,
            detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Create comparison_results table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparison_results (
            id SERIAL PRIMARY KEY,
            file_id INTEGER REFERENCES files(id),
            type TEXT NOT NULL,
            row_num INTEGER,
            column_name TEXT,
            old_value TEXT,
            new_value TEXT,
            change_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        conn.commit()
        return True
    except Exception as e:
        print(f"Supabase tablo oluşturma hatası: {e}")
        return False
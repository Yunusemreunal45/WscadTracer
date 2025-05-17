import os
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime

def get_sqlite_connection(db_file="wscad_comparison.db"):
    """Get connection to SQLite database"""
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        print(f"Connected to SQLite database: {db_file}")
        return conn
    except Exception as e:
        print(f"SQLite connection error: {e}")
        return None

def get_supabase_connection():
    """Get connection to Supabase PostgreSQL database"""
    # Load environment variables from .env file
    load_dotenv()
    
    try:
        # Connect to Supabase PostgreSQL using direct connection parameters
        print("Connecting to Supabase PostgreSQL...")
        
        # These should match your Supabase project settings
        host = "db.jnyxsuosikivbywxjzvr.supabase.co"
        port = "5432"
        database = "postgres"
        user = "postgres"
        password = "Yunus.2002"  # Bu şifreyi gerçek bir uygulamada bu şekilde saklamak güvenlik riski oluşturur
        
        # Connect with direct parameters
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password
        )
        
        conn.autocommit = True
        print("Connected to Supabase PostgreSQL")
        return conn
    except Exception as e:
        print(f"Supabase PostgreSQL connection error: {e}")
        return None

def create_supabase_tables(pg_conn):
    """Create tables in Supabase PostgreSQL"""
    if not pg_conn:
        return False
    
    try:
        cursor = pg_conn.cursor()
        
        # Users table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )
        ''')
        
        # Files table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            filesize INTEGER,
            detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed BOOLEAN DEFAULT FALSE,
            current_revision INTEGER DEFAULT 1
        )
        ''')
        
        # File revisions table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_revisions (
            id SERIAL PRIMARY KEY,
            file_id INTEGER NOT NULL,
            revision_number INTEGER NOT NULL,
            revision_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            revision_path TEXT NOT NULL,
            FOREIGN KEY (file_id) REFERENCES files(id)
        )
        ''')
        
        # Comparison results table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS comparisons (
            id SERIAL PRIMARY KEY,
            file_id INTEGER NOT NULL,
            revision1_id INTEGER NOT NULL,
            revision2_id INTEGER NOT NULL,
            changes_count INTEGER,
            comparison_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (file_id) REFERENCES files(id),
            FOREIGN KEY (revision1_id) REFERENCES file_revisions(id),
            FOREIGN KEY (revision2_id) REFERENCES file_revisions(id)
        )
        ''')
        
        # Activity logs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS activity_logs (
            id SERIAL PRIMARY KEY,
            username TEXT,
            activity TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        print("Supabase PostgreSQL tables created successfully")
        return True
    except Exception as e:
        print(f"Error creating tables in Supabase PostgreSQL: {e}")
        return False

def migrate_table(sqlite_conn, pg_conn, table_name, columns, column_types=None):
    """Migrate a table from SQLite to Supabase PostgreSQL"""
    if not sqlite_conn or not pg_conn:
        return False
    
    try:
        sqlite_cursor = sqlite_conn.cursor()
        pg_cursor = pg_conn.cursor()
        
        # Get data from SQLite
        sqlite_cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
        rows = sqlite_cursor.fetchall()
        
        if not rows:
            print(f"No data in table {table_name}")
            return True
        
        # Insert data into PostgreSQL
        for row in rows:
            # Convert row to dictionary
            row_dict = dict(row)
            
            # Build SQL INSERT statement
            placeholders = ', '.join(['%s'] * len(columns))
            column_list = ', '.join(columns)
            sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
            
            # Convert row to list of values for PostgreSQL
            values = [row_dict[col] for col in columns]
            
            # Execute INSERT
            pg_cursor.execute(sql, values)
        
        print(f"Migrated {len(rows)} rows from table {table_name}")
        return True
    except Exception as e:
        print(f"Error migrating table {table_name}: {e}")
        return False

def migrate_data(sqlite_conn, pg_conn):
    """Migrate all data from SQLite to Supabase PostgreSQL"""
    if not sqlite_conn or not pg_conn:
        return False
    
    # Define tables and columns to migrate
    tables = {
        "users": ["username", "password", "created_at", "last_login"],
        "files": ["filename", "filepath", "filesize", "detected_time", "processed", "current_revision"],
        "file_revisions": ["file_id", "revision_number", "revision_date", "revision_path"],
        "comparisons": ["file_id", "revision1_id", "revision2_id", "changes_count", "comparison_date"],
        "activity_logs": ["username", "activity", "timestamp"]
    }
    
    success = True
    for table, columns in tables.items():
        if not migrate_table(sqlite_conn, pg_conn, table, columns):
            success = False
    
    return success

def main():
    """Main migration function"""
    # Get connections
    sqlite_conn = get_sqlite_connection()
    pg_conn = get_supabase_connection()
    
    if not sqlite_conn:
        print("Failed to connect to SQLite database")
        return
    
    if not pg_conn:
        print("Failed to connect to Supabase PostgreSQL database")
        return
    
    # Create tables in Supabase PostgreSQL
    if not create_supabase_tables(pg_conn):
        print("Failed to create tables in Supabase PostgreSQL")
        return
    
    # Migrate data
    if migrate_data(sqlite_conn, pg_conn):
        print("Data migration successful")
    else:
        print("Data migration failed")
    
    # Close connections
    sqlite_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
import os
import sqlite3
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import datetime

def get_sqlite_connection(db_file="wscad_comparison.db"):
    """Get connection to SQLite database"""
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        print(f"Connected to SQLite database: {db_file}")
        return conn
    except Exception as e:
        print(f"SQLite connection error: {e}")
        return None

def get_supabase_connection():
    """Get connection to Supabase PostgreSQL database"""
    try:
        if not all([
            os.getenv('SUPABASE_HOST'),
            os.getenv('SUPABASE_DATABASE'),
            os.getenv('SUPABASE_USER'),
            os.getenv('SUPABASE_PASSWORD')
        ]):
            raise ValueError("Missing Supabase environment variables")

        connection = psycopg2.connect(
            host=os.getenv('SUPABASE_HOST'),
            database=os.getenv('SUPABASE_DATABASE'),
            user=os.getenv('SUPABASE_USER'),
            password=os.getenv('SUPABASE_PASSWORD')
        )
        return connection
    except Exception as e:
        print(f"Supabase connection error: {e}")
        return None

def close_supabase_connection(connection):
    if connection:
        try:
            connection.close()
            print("Supabase connection closed successfully")
        except Exception as e:
            print(f"Error closing Supabase connection: {e}")

def create_supabase_tables(conn):
    """Create necessary tables in Supabase"""
    try:
        cursor = conn.cursor()

        # Create comparison results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comparison_results (
                id SERIAL PRIMARY KEY,
                file1_name TEXT,
                file2_name TEXT,
                comparison_data TEXT,
                data_format TEXT DEFAULT 'csv',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create user activity table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                id SERIAL PRIMARY KEY,
                username TEXT,
                action TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        return True
    except Exception as e:
        print(f"Error creating Supabase tables: {e}")
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

    # Migrate data
    if migrate_data(sqlite_conn, pg_conn):
        print("Data migration successful")
    else:
        print("Data migration failed")

    # Close connections
    sqlite_conn.close()
    close_supabase_connection(pg_conn)

if __name__ == "__main__":
    main()
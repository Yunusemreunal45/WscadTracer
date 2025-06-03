import sqlite3
import os

# Database file path
db_file = 'wscad_comparison.db'

# Connect to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

print("🔄 Starting database reset...")

try:
    # 1. Reset the sync status and supabase_id for all active projects
    cursor.execute("UPDATE projects SET sync_status = NULL, supabase_id = NULL WHERE is_active = 1")
    print(f"✅ Reset {cursor.rowcount} projects in the database")
    
    # 2. Drop and recreate the wscad_comparisons table to match Supabase schema
    cursor.execute("DROP TABLE IF EXISTS wscad_comparisons")
    cursor.execute("""
        CREATE TABLE wscad_comparisons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            changes_count INTEGER DEFAULT 0,
            comparison_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            supabase_saved BOOLEAN DEFAULT 0,
            supabase_comparison_id TEXT,
            created_by TEXT
        )
    """)
    print("✅ Recreated wscad_comparisons table with updated schema")
    
    # 3. Reset all supabase_saved flags in project_comparisons
    cursor.execute("UPDATE project_comparisons SET supabase_saved = 0, supabase_revision_id = NULL")
    print(f"✅ Reset {cursor.rowcount} project comparison records")
    
    # Commit all changes
    conn.commit()
    print("✅ All changes committed successfully")
    
except Exception as e:
    # Rollback in case of error
    conn.rollback()
    print(f"❌ Error during database reset: {e}")
    
finally:
    # Close the connection
    conn.close()
    print("✅ Database connection closed")

print("🎉 Database reset complete!")


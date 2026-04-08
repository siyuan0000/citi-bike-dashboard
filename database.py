import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_NAME = str(BASE_DIR / "citibike.db")

def get_db_connection():
    # Connect to local SQLite database (easily convertible to AWS RDS later)
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create Station table to store static information
    c.execute('''
        CREATE TABLE IF NOT EXISTS Station (
            station_id TEXT PRIMARY KEY,
            name TEXT,
            lat REAL,
            lon REAL,
            capacity INTEGER
        )
    ''')
    
    # Create Status table to store real-time dynamic data (updated every 30s)
    c.execute('''
        CREATE TABLE IF NOT EXISTS Status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            num_bikes_available INTEGER,
            num_docks_available INTEGER,
            grab_time INTEGER,
            FOREIGN KEY (station_id) REFERENCES Station (station_id)
        )
    ''')

    # Backward-compatible schema migration for existing local DB files.
    c.execute("PRAGMA table_info(Status)")
    existing_columns = {row[1] for row in c.fetchall()}
    required_columns = {
        "is_installed": "INTEGER",
        "is_renting": "INTEGER",
        "is_returning": "INTEGER",
        "num_bikes_disabled": "INTEGER",
        "num_docks_disabled": "INTEGER",
    }

    for col_name, col_type in required_columns.items():
        if col_name not in existing_columns:
            c.execute(f"ALTER TABLE Status ADD COLUMN {col_name} {col_type}")
    
    conn.commit()
    conn.close()
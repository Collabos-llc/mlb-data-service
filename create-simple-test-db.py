#!/usr/bin/env python3
"""
Create simple test database for DBeaver connection testing
"""

import sqlite3
import os
from datetime import datetime

def create_simple_test_db():
    """Create minimal test database to verify DBeaver connection"""
    
    # Try multiple locations
    locations = [
        "/mnt/e/test_db.db",
        "/mnt/c/temp/test_db.db", 
        "/mnt/c/Users/Public/test_db.db"
    ]
    
    for db_path in locations:
        try:
            # Create directory if needed
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Remove existing file
            if os.path.exists(db_path):
                os.remove(db_path)
            
            # Create simple database
            conn = sqlite3.connect(db_path)
            
            # Create simple test table
            conn.execute("""
                CREATE TABLE test_players (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    team TEXT,
                    home_runs INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data
            test_data = [
                (1, 'Aaron Judge', 'NYY', 62),
                (2, 'Shohei Ohtani', 'LAD', 44),
                (3, 'Mookie Betts', 'LAD', 35),
                (4, 'Mike Trout', 'LAA', 40),
                (5, 'Ronald Acuna Jr.', 'ATL', 41)
            ]
            
            conn.executemany("""
                INSERT INTO test_players (id, name, team, home_runs) 
                VALUES (?, ?, ?, ?)
            """, test_data)
            
            conn.commit()
            conn.close()
            
            # Verify the database
            file_size = os.path.getsize(db_path)
            
            # Convert WSL path to Windows path
            if db_path.startswith('/mnt/c/'):
                windows_path = db_path.replace('/mnt/c/', 'C:\\').replace('/', '\\')
            elif db_path.startswith('/mnt/e/'):
                windows_path = db_path.replace('/mnt/e/', 'E:\\').replace('/', '\\')
            else:
                windows_path = db_path
            
            print(f"✅ Created: {db_path}")
            print(f"📍 Windows path: {windows_path}")
            print(f"📊 Size: {file_size} bytes")
            
            # Test reading the data back
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("SELECT name, team, home_runs FROM test_players ORDER BY home_runs DESC")
            players = cursor.fetchall()
            
            print(f"📋 Contains {len(players)} test records:")
            for player in players:
                print(f"   {player[0]} ({player[1]}): {player[2]} HR")
            
            conn.close()
            print()
            
        except Exception as e:
            print(f"❌ Failed to create {db_path}: {e}")
            continue
    
    print("🎯 TRY THESE PATHS IN DBEAVER:")
    print("==============================")
    print("1. E:\\test_db.db")
    print("2. C:\\temp\\test_db.db") 
    print("3. C:\\Users\\Public\\test_db.db")
    print()
    print("🔧 If none work, the issue might be:")
    print("   • DBeaver SQLite driver not installed")
    print("   • File permissions")
    print("   • DBeaver running as different user")

if __name__ == "__main__":
    print("🧪 Simple DBeaver Connection Test")
    print("=================================")
    create_simple_test_db()
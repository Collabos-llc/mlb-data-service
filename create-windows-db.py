#!/usr/bin/env python3
"""
Create StatEdge database directly in Windows-accessible location
"""

import sqlite3
import pandas as pd
from datetime import datetime
import pybaseball
import os

def create_windows_database():
    """Create database in Windows temp folder"""
    
    # Windows accessible path
    db_path = "/mnt/c/temp/statedge_mlb.db"
    
    print(f"🔧 Creating database at: {db_path}")
    print(f"📍 Windows path: C:\\temp\\statedge_mlb.db")
    
    # Remove existing file
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create new database
    conn = sqlite3.connect(db_path)
    
    print("📊 Collecting fresh MLB data...")
    
    try:
        # Get 2025 batting stats
        batting_data = pybaseball.batting_stats(2025, qual=50)
        
        if batting_data is not None and len(batting_data) > 0:
            # Clean up column names and select key stats
            batting_clean = pd.DataFrame({
                'player_name': batting_data['Name'],
                'team': batting_data['Team'], 
                'games': batting_data['G'],
                'plate_appearances': batting_data['PA'],
                'home_runs': batting_data['HR'],
                'rbi': batting_data['RBI'],
                'stolen_bases': batting_data['SB'],
                'batting_avg': batting_data['AVG'].round(3),
                'on_base_pct': batting_data['OBP'].round(3),
                'slugging_pct': batting_data['SLG'].round(3),
                'ops': batting_data['OPS'].round(3),
                'wrc_plus': batting_data['wRC+'],
                'war': batting_data['WAR'].round(1),
                'collection_date': datetime.now()
            })
            
            # Save to database
            batting_clean.to_sql('mlb_batting_2025', conn, if_exists='replace', index=False)
            
            print(f"✅ Saved {len(batting_clean)} player records")
            
            # Show top performers
            top_players = batting_clean.nlargest(5, 'war')
            print("\n🏆 Top 5 Players by WAR:")
            for _, player in top_players.iterrows():
                print(f"   {player['player_name']} ({player['team']}): {player['home_runs']} HR, {player['batting_avg']} AVG, {player['war']} WAR")
    
    except Exception as e:
        print(f"⚠️ Error getting live data: {e}")
        print("Creating sample data instead...")
        
        # Create sample data
        sample_data = pd.DataFrame({
            'player_name': ['Aaron Judge', 'Shohei Ohtani', 'Mookie Betts', 'Mike Trout', 'Ronald Acuna Jr.'],
            'team': ['NYY', 'LAD', 'LAD', 'LAA', 'ATL'],
            'games': [150, 145, 142, 120, 155],
            'plate_appearances': [650, 625, 600, 500, 675],
            'home_runs': [62, 44, 35, 40, 41],
            'rbi': [131, 95, 82, 80, 106],
            'stolen_bases': [16, 10, 12, 18, 73],
            'batting_avg': [0.311, 0.304, 0.269, 0.283, 0.337],
            'on_base_pct': [0.425, 0.412, 0.367, 0.369, 0.416],
            'slugging_pct': [0.686, 0.654, 0.491, 0.544, 0.596],
            'ops': [1.111, 1.066, 0.858, 0.913, 1.012],
            'wrc_plus': [207, 180, 140, 147, 168],
            'war': [11.0, 9.6, 6.8, 7.2, 8.9],
            'collection_date': [datetime.now()] * 5
        })
        
        sample_data.to_sql('mlb_batting_2025', conn, if_exists='replace', index=False)
        print("✅ Created sample data with 5 star players")
    
    # Create additional tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mlb_teams (
            team_code TEXT PRIMARY KEY,
            team_name TEXT,
            city TEXT,
            division TEXT,
            league TEXT
        )
    """)
    
    # Insert team data
    teams_data = [
        ('NYY', 'Yankees', 'New York', 'AL East', 'American League'),
        ('LAD', 'Dodgers', 'Los Angeles', 'NL West', 'National League'),
        ('ATL', 'Braves', 'Atlanta', 'NL East', 'National League'),
        ('LAA', 'Angels', 'Los Angeles', 'AL West', 'American League'),
        ('HOU', 'Astros', 'Houston', 'AL West', 'American League')
    ]
    
    conn.executemany("""
        INSERT OR REPLACE INTO mlb_teams VALUES (?, ?, ?, ?, ?)
    """, teams_data)
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Database created successfully!")
    print(f"📍 Windows path: C:\\temp\\statedge_mlb.db")
    print(f"📍 Full path: {os.path.abspath(db_path)}")
    
    # Verify database
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT COUNT(*) FROM mlb_batting_2025")
    count = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n📊 Database contains:")
    print(f"   • {count} player records")
    print(f"   • Tables: {', '.join(tables)}")
    
    conn.close()
    
    return db_path

if __name__ == "__main__":
    print("🏢 StatEdge Windows Database Creator")
    print("====================================")
    
    db_path = create_windows_database()
    
    print(f"\n🎯 Next steps:")
    print(f"1. Open DBeaver")
    print(f"2. New Connection → SQLite")
    print(f"3. Path: C:\\temp\\statedge_mlb.db")
    print(f"4. Test Connection → Should work!")
    print(f"5. Explore the mlb_batting_2025 table")
#!/usr/bin/env python3
"""
SQLite Fallback for StatEdge - works without Docker
"""

import sqlite3
import pandas as pd
from datetime import datetime
import pybaseball
import os

def create_sqlite_database():
    """Create SQLite database for StatEdge data"""
    
    db_path = "statedge_mlb_data.db"
    
    print(f"🔧 Creating SQLite database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fangraphs_batting_2025 (
            player_name TEXT,
            team TEXT,
            games INTEGER,
            plate_appearances INTEGER,
            home_runs INTEGER,
            batting_avg REAL,
            on_base_pct REAL,
            slugging_pct REAL,
            ops REAL,
            wrc_plus INTEGER,
            war REAL,
            collection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fangraphs_pitching_2025 (
            player_name TEXT,
            team TEXT,
            games INTEGER,
            innings_pitched REAL,
            wins INTEGER,
            losses INTEGER,
            era REAL,
            whip REAL,
            strikeouts INTEGER,
            fip REAL,
            war REAL,
            collection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS statcast_data (
            game_date TEXT,
            player_name TEXT,
            batter TEXT,
            pitcher TEXT,
            events TEXT,
            description TEXT,
            launch_speed REAL,
            launch_angle REAL,
            hit_distance_sc REAL,
            collection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    
    print("✅ SQLite database created!")
    return db_path

def collect_sample_data():
    """Collect some sample MLB data"""
    
    db_path = create_sqlite_database()
    
    print("📊 Collecting sample FanGraphs batting data...")
    
    try:
        # Get recent batting stats
        batting_data = pybaseball.batting_stats(2025, qual=50)
        
        if batting_data is not None and len(batting_data) > 0:
            # Select key columns
            batting_subset = batting_data[['Name', 'Team', 'G', 'PA', 'HR', 'AVG', 'OBP', 'SLG', 'OPS', 'wRC+', 'WAR']].head(20)
            batting_subset.columns = ['player_name', 'team', 'games', 'plate_appearances', 'home_runs', 'batting_avg', 'on_base_pct', 'slugging_pct', 'ops', 'wrc_plus', 'war']
            
            # Save to SQLite
            conn = sqlite3.connect(db_path)
            batting_subset.to_sql('fangraphs_batting_2025', conn, if_exists='replace', index=False)
            conn.close()
            
            print(f"✅ Saved {len(batting_subset)} batting records")
        
        print("📊 Sample data collection complete!")
        
        # Show what we collected
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM fangraphs_batting_2025")
        count = cursor.fetchone()[0]
        print(f"📋 Database now contains {count} batting records")
        
        # Show top 5 players
        cursor = conn.execute("SELECT player_name, team, home_runs, batting_avg, war FROM fangraphs_batting_2025 ORDER BY war DESC LIMIT 5")
        top_players = cursor.fetchall()
        
        print("\n🏆 Top 5 Players by WAR:")
        for player in top_players:
            print(f"   {player[0]} ({player[1]}): {player[2]} HR, .{int(player[3]*1000):03d} AVG, {player[4]:.1f} WAR")
        
        conn.close()
        
        print(f"\n🎯 You can now connect to: {db_path}")
        print("📋 Use SQLite browser or DBeaver with SQLite driver")
        
        return db_path
        
    except Exception as e:
        print(f"❌ Error collecting data: {e}")
        return None

if __name__ == "__main__":
    print("🏢 StatEdge SQLite Fallback")
    print("==========================")
    print("This creates a local SQLite database with MLB data")
    print("(Use this if Docker is having issues)")
    print()
    
    db_path = collect_sample_data()
    
    if db_path:
        print(f"\n✅ Success! Database created: {db_path}")
        print("\n🔧 To connect with DBeaver:")
        print("1. New Connection > SQLite")
        print(f"2. Path: {os.path.abspath(db_path)}")
        print("3. Test Connection")
    else:
        print("\n❌ Failed to create database")
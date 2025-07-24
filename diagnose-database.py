#!/usr/bin/env python3
"""
Diagnose StatEdge Database State and Plan Full Population
"""

import sqlite3
import os
from datetime import datetime

def diagnose_database():
    """Analyze current database state and plan comprehensive population"""
    
    # Check all possible database locations
    db_locations = [
        "/mnt/e/statedge_mlb.db",
        "/mnt/e/StatEdge/statedge_mlb_full.db", 
        "/mnt/e/test_db.db",
        "/mnt/c/temp/test_db.db"
    ]
    
    working_db = None
    for db_path in db_locations:
        if os.path.exists(db_path):
            working_db = db_path
            break
    
    if not working_db:
        print("❌ No database found!")
        return None
    
    print(f"🔍 Analyzing database: {working_db}")
    print(f"📍 Windows path: {working_db.replace('/mnt/e/', 'E:\\').replace('/mnt/c/', 'C:\\').replace('/', '\\')}")
    
    # Connect and analyze
    conn = sqlite3.connect(working_db)
    
    # Get all tables
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n📊 Database Analysis:")
    print(f"{'='*50}")
    print(f"📋 Tables found: {len(tables)}")
    
    total_records = 0
    table_analysis = {}
    
    for table in tables:
        try:
            # Get record count
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_records += count
            
            # Get table structure
            cursor = conn.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            # Get sample data
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT 3")
            sample_data = cursor.fetchall()
            
            table_analysis[table] = {
                'count': count,
                'columns': len(columns),
                'has_data': count > 0,
                'sample': sample_data[:1] if sample_data else []
            }
            
            print(f"\n📊 {table}:")
            print(f"   Records: {count:,}")
            print(f"   Columns: {len(columns)}")
            if sample_data:
                print(f"   Sample: {sample_data[0][:3]}..." if len(sample_data[0]) > 3 else f"   Sample: {sample_data[0]}")
            
        except Exception as e:
            print(f"❌ Error analyzing {table}: {e}")
    
    print(f"\n🎯 SUMMARY:")
    print(f"   Total Records: {total_records:,}")
    print(f"   Total Tables: {len(tables)}")
    
    # Identify what's missing/needs population
    expected_tables = {
        'fangraphs_batting_2025': {'target': 600, 'description': 'MLB batters with full stats'},
        'fangraphs_pitching_2025': {'target': 600, 'description': 'MLB pitchers with full stats'},
        'statcast_data': {'target': 1000, 'description': 'Pitch-by-pitch data'},
        'mlb_teams': {'target': 30, 'description': 'All MLB teams'},
        'collection_log': {'target': 10, 'description': 'Data collection history'}
    }
    
    print(f"\n📋 DATA POPULATION PLAN:")
    print(f"{'='*50}")
    
    missing_data = []
    for table_name, info in expected_tables.items():
        current_count = table_analysis.get(table_name, {}).get('count', 0)
        target_count = info['target']
        
        if current_count < target_count:
            missing_data.append({
                'table': table_name,
                'current': current_count,
                'target': target_count,
                'description': info['description']
            })
            status = "❌ NEEDS DATA"
        else:
            status = "✅ POPULATED"
            
        print(f"{status} {table_name}: {current_count:,}/{target_count:,} - {info['description']}")
    
    conn.close()
    
    return {
        'db_path': working_db,
        'tables': table_analysis,
        'total_records': total_records,
        'missing_data': missing_data
    }

if __name__ == "__main__":
    print("🔍 StatEdge Database Diagnosis")
    print("==============================")
    
    analysis = diagnose_database()
    
    if analysis and analysis['missing_data']:
        print(f"\n🚀 NEXT STEPS - Need to populate:")
        for item in analysis['missing_data']:
            print(f"   • {item['table']}: {item['target'] - item['current']:,} more records")
        
        print(f"\n✅ Ready to build comprehensive data collection pipeline!")
    elif analysis:
        print(f"\n🎉 Database appears to be well populated!")
        print(f"✅ Ready for analytics and querying!")
    else:
        print(f"\n❌ Need to create database first!")
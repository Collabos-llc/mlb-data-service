#!/usr/bin/env python3
"""
Test database connection and create sample data for StatEdge
"""

import os
import sys
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

def test_database_connection():
    """Test connection to StatEdge PostgreSQL database"""
    
    # Database connection string
    db_url = 'postgresql://statedge_user:statedge_secure_2024@localhost:5439/mlb_data'
    
    print("🔗 Testing StatEdge database connection...")
    print(f"📍 Connection string: {db_url}")
    
    try:
        # Create engine
        engine = create_engine(db_url)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to PostgreSQL: {version}")
            
        # Test creating a sample table
        sample_data = pd.DataFrame({
            'player_name': ['Mike Trout', 'Mookie Betts', 'Aaron Judge'],
            'team': ['LAA', 'LAD', 'NYY'],
            'home_runs': [40, 35, 62],
            'batting_avg': [0.283, 0.269, 0.311],
            'collection_date': [datetime.now()] * 3
        })
        
        # Save to database
        sample_data.to_sql(
            'test_batting_sample', 
            engine, 
            if_exists='replace',
            index=False
        )
        
        print("✅ Successfully created test table 'test_batting_sample'")
        
        # Read back from database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_batting_sample"))
            count = result.fetchone()[0]
            print(f"✅ Test table contains {count} records")
            
            # Show table structure
            result = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_batting_sample'"))
            columns = result.fetchall()
            print("📋 Table structure:")
            for col in columns:
                print(f"   {col[0]}: {col[1]}")
        
        print("\n🎯 Database connection test PASSED!")
        print("🔧 Your DBeaver connection should work with these settings:")
        print("   Host: localhost")
        print("   Port: 5439") 
        print("   Database: mlb_data")
        print("   Username: statedge_user")
        print("   Password: statedge_secure_2024")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\n🔧 Troubleshooting steps:")
        print("   1. Make sure Docker is running")
        print("   2. Run: ./fix-docker-and-database.sh")
        print("   3. Check if PostgreSQL container is running: docker ps | grep postgres")
        print("   4. Verify port 5439 is accessible: netstat -tln | grep 5439")
        
        return False

if __name__ == "__main__":
    print("🏢 StatEdge Database Connection Test")
    print("=" * 40)
    
    # Test database connection
    success = test_database_connection()
    
    if success:
        print("\n✅ Ready to collect and store MLB data!")
        sys.exit(0)
    else:
        print("\n❌ Database connection needs to be fixed first")
        sys.exit(1)
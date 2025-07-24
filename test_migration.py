#!/usr/bin/env python3
"""
Test Statcast Migration
======================

Test a small batch to debug migration issues.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_migration():
    """Test small migration to debug issues"""
    
    # Digital Ocean connection
    do_conn_params = {
        'host': os.getenv('DO_DB_HOST', 'baseball-db-do-user-23895569-0.j.db.ondigitalocean.com'),
        'port': int(os.getenv('DO_DB_PORT', 25060)),
        'user': os.getenv('DO_DB_USER', 'doadmin'),
        'password': os.getenv('DO_DB_PASSWORD'),
        'database': os.getenv('DO_DB_NAME', 'defaultdb'),
        'sslmode': 'require'
    }
    
    # Local connection
    local_conn_params = {
        'host': 'localhost',
        'port': 5439,
        'user': 'mlb_user',
        'password': 'mlb_secure_pass_2024',
        'database': 'mlb_data'
    }
    
    try:
        # Test DO connection
        logger.info("Testing DO connection...")
        do_conn = psycopg2.connect(**do_conn_params)
        do_cursor = do_conn.cursor(cursor_factory=RealDictCursor)
        
        # Get sample data
        do_cursor.execute("SELECT * FROM statcast LIMIT 1")
        sample_row = do_cursor.fetchone()
        
        logger.info(f"Sample row columns: {list(sample_row.keys())}")
        logger.info(f"Sample row: {dict(sample_row)}")
        
        do_cursor.close()
        do_conn.close()
        
        # Test local connection
        logger.info("Testing local connection...")
        local_conn = psycopg2.connect(**local_conn_params)
        local_cursor = local_conn.cursor()
        
        # Check local table structure
        local_cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'statcast' 
            ORDER BY ordinal_position
        """)
        
        local_columns = [row[0] for row in local_cursor.fetchall()]
        logger.info(f"Local table columns ({len(local_columns)}): {local_columns[:10]}...")
        
        local_cursor.close()
        local_conn.close()
        
        logger.info("✅ Both connections working")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_migration()
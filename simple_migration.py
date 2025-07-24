#!/usr/bin/env python3
"""
Simple Statcast Migration
========================

Direct migration using pg_dump approach.
"""

import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_with_pgdump():
    """Migrate using pg_dump and psql"""
    logger.info("🚀 Starting Statcast migration using pg_dump...")
    start_time = datetime.now()
    
    try:
        # Step 1: Export data from Digital Ocean
        logger.info("📤 Exporting data from Digital Ocean...")
        
        dump_command = [
            'pg_dump',
            '-h', 'baseball-db-do-user-23895569-0.j.db.ondigitalocean.com',
            '-p', '25060',
            '-U', 'doadmin',
            '-d', 'defaultdb',
            '--data-only',
            '--table=statcast',
            '--file=statcast_data.sql'
        ]
        
        env = {'PGPASSWORD': os.getenv('DO_DB_PASSWORD')}
        
        result = subprocess.run(dump_command, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"pg_dump failed: {result.stderr}")
            return False
        
        logger.info("✅ Data exported successfully")
        
        # Step 2: Import data to local database
        logger.info("📥 Importing data to local database...")
        
        import_command = [
            'psql',
            '-h', 'localhost',
            '-p', '5439',
            '-U', 'mlb_user',
            '-d', 'mlb_data',
            '-f', 'statcast_data.sql'
        ]
        
        env = {'PGPASSWORD': 'mlb_secure_pass_2024'}
        
        result = subprocess.run(import_command, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"psql import failed: {result.stderr}")
            return False
        
        logger.info("✅ Data imported successfully")
        
        # Step 3: Validate
        logger.info("🔍 Validating migration...")
        
        validate_command = [
            'psql',
            '-h', 'localhost',
            '-p', '5439',
            '-U', 'mlb_user',
            '-d', 'mlb_data',
            '-c', 'SELECT COUNT(*) FROM statcast'
        ]
        
        result = subprocess.run(validate_command, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            output = result.stdout.strip()
            # Extract count from output
            lines = output.split('\n')
            for line in lines:
                line = line.strip()
                if line.isdigit():
                    count = int(line)
                    logger.info(f"📊 Local database now has {count:,} Statcast records")
                    break
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"🎉 Migration completed in {duration}")
        
        # Cleanup
        import os
        if os.path.exists('statcast_data.sql'):
            os.remove('statcast_data.sql')
            logger.info("🧹 Cleaned up temporary files")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

if __name__ == "__main__":
    success = migrate_with_pgdump()
    if success:
        print("\n✅ Statcast migration completed successfully!")
    else:
        print("\n❌ Statcast migration failed!")
    exit(0 if success else 1)
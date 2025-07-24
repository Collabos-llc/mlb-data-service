#!/usr/bin/env python3
"""
Statcast Data Migration
======================

Migrates all Statcast data from Digital Ocean database to local enhanced database.
"""

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import logging
from datetime import datetime
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StatcastMigrator:
    """Handles migration of Statcast data between databases"""
    
    def __init__(self):
        # Digital Ocean connection
        self.do_conn_params = {
            'host': os.getenv('DO_DB_HOST', 'baseball-db-do-user-23895569-0.j.db.ondigitalocean.com'),
            'port': int(os.getenv('DO_DB_PORT', 25060)),
            'user': os.getenv('DO_DB_USER', 'doadmin'),
            'password': os.getenv('DO_DB_PASSWORD'),
            'database': os.getenv('DO_DB_NAME', 'defaultdb'),
            'sslmode': 'require'
        }
        
        # Local connection
        self.local_conn_params = {
            'host': 'localhost',
            'port': 5439,
            'user': 'mlb_user',
            'password': 'mlb_secure_pass_2024',
            'database': 'mlb_data'
        }
        
    def get_do_connection(self):
        """Get Digital Ocean database connection"""
        return psycopg2.connect(**self.do_conn_params)
    
    def get_local_connection(self):
        """Get local database connection"""
        return psycopg2.connect(**self.local_conn_params)
    
    def analyze_source_data(self):
        """Analyze source data in Digital Ocean"""
        logger.info("Analyzing source Statcast data...")
        
        try:
            do_conn = self.get_do_connection()
            cursor = do_conn.cursor(cursor_factory=RealDictCursor)
            
            # Get overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(game_date) as earliest_date,
                    MAX(game_date) as latest_date,
                    COUNT(DISTINCT game_pk) as unique_games
                FROM statcast
            """)
            
            stats = dict(cursor.fetchone())
            logger.info(f"Source data analysis:")
            logger.info(f"  Total records: {stats['total_records']:,}")
            logger.info(f"  Date range: {stats['earliest_date']} to {stats['latest_date']}")
            logger.info(f"  Unique games: {stats['unique_games']:,}")
            
            # Get monthly breakdown
            cursor.execute("""
                SELECT 
                    EXTRACT(YEAR FROM game_date) as year,
                    EXTRACT(MONTH FROM game_date) as month,
                    COUNT(*) as records
                FROM statcast 
                GROUP BY EXTRACT(YEAR FROM game_date), EXTRACT(MONTH FROM game_date)
                ORDER BY year, month
            """)
            
            monthly_data = cursor.fetchall()
            logger.info(f"Monthly breakdown:")
            for row in monthly_data:
                logger.info(f"  {int(row['year'])}-{int(row['month']):02d}: {row['records']:,} records")
            
            cursor.close()
            do_conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to analyze source data: {e}")
            return None
    
    def clear_local_statcast(self):
        """Clear existing Statcast data in local database"""
        logger.info("Clearing existing local Statcast data...")
        
        try:
            local_conn = self.get_local_connection()
            cursor = local_conn.cursor()
            
            # Get current count
            cursor.execute("SELECT COUNT(*) FROM statcast")
            current_count = cursor.fetchone()[0]
            logger.info(f"Current local records: {current_count:,}")
            
            # Clear table
            cursor.execute("TRUNCATE TABLE statcast")
            local_conn.commit()
            
            logger.info("Local Statcast table cleared")
            
            cursor.close()
            local_conn.close()
            
        except Exception as e:
            logger.error(f"Failed to clear local data: {e}")
            raise
    
    def migrate_statcast_batch(self, year: int, month: int, batch_size: int = 10000):
        """Migrate Statcast data for a specific month in batches"""
        logger.info(f"Migrating {year}-{month:02d} Statcast data...")
        
        try:
            # Get source data
            do_conn = self.get_do_connection()
            do_cursor = do_conn.cursor(cursor_factory=RealDictCursor)
            
            # Count records for this month
            do_cursor.execute("""
                SELECT COUNT(*) FROM statcast 
                WHERE EXTRACT(YEAR FROM game_date) = %s 
                AND EXTRACT(MONTH FROM game_date) = %s
            """, (year, month))
            
            total_records = do_cursor.fetchone()[0]
            logger.info(f"  Found {total_records:,} records for {year}-{month:02d}")
            
            if total_records == 0:
                return 0
            
            # Get local connection
            local_conn = self.get_local_connection()
            local_cursor = local_conn.cursor()
            
            # Process in batches
            offset = 0
            total_migrated = 0
            
            while offset < total_records:
                # Fetch batch from DO
                do_cursor.execute("""
                    SELECT * FROM statcast 
                    WHERE EXTRACT(YEAR FROM game_date) = %s 
                    AND EXTRACT(MONTH FROM game_date) = %s
                    ORDER BY game_pk, at_bat_number, pitch_number
                    LIMIT %s OFFSET %s
                """, (year, month, batch_size, offset))
                
                batch_data = do_cursor.fetchall()
                
                if not batch_data:
                    break
                
                # Prepare data for insertion
                columns = list(batch_data[0].keys())
                values = []
                
                for row in batch_data:
                    row_values = []
                    for col in columns:
                        value = row[col]
                        row_values.append(value)
                    values.append(tuple(row_values))
                
                # Build insert query
                column_names = ['"' + col + '"' if ' ' in col or '-' in col or col.startswith(tuple('0123456789')) else col for col in columns]
                placeholders = ','.join(['%s'] * len(columns))
                
                insert_query = """
                    INSERT INTO statcast ({})
                    VALUES ({})
                    ON CONFLICT (game_pk, at_bat_number, pitch_number) DO NOTHING
                """.format(','.join(column_names), placeholders)
                
                # Insert batch
                execute_values(local_cursor, insert_query, values, page_size=1000)
                local_conn.commit()
                
                batch_migrated = len(batch_data)
                total_migrated += batch_migrated
                offset += batch_size
                
                logger.info(f"    Migrated batch: {batch_migrated:,} records ({total_migrated:,}/{total_records:,})")
            
            logger.info(f"  ✅ Completed {year}-{month:02d}: {total_migrated:,} records migrated")
            
            # Clean up connections
            do_cursor.close()
            do_conn.close()
            local_cursor.close()
            local_conn.close()
            
            return total_migrated
            
        except Exception as e:
            logger.error(f"Failed to migrate {year}-{month:02d} data: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    def migrate_all_statcast(self):
        """Migrate all Statcast data from Digital Ocean to local database"""
        logger.info("🚀 Starting complete Statcast data migration...")
        start_time = datetime.now()
        
        try:
            # Analyze source data
            source_stats = self.analyze_source_data()
            if not source_stats:
                logger.error("Failed to analyze source data")
                return False
            
            # Clear local data
            self.clear_local_statcast()
            
            # Define months to migrate (2025 season)
            months_to_migrate = [
                (2025, 3),  # March
                (2025, 4),  # April
                (2025, 5),  # May
                (2025, 6),  # June
                (2025, 7),  # July
            ]
            
            total_migrated = 0
            
            # Migrate each month
            for year, month in months_to_migrate:
                try:
                    month_migrated = self.migrate_statcast_batch(year, month)
                    total_migrated += month_migrated
                except Exception as e:
                    logger.error(f"Failed to migrate {year}-{month:02d}: {e}")
                    continue
            
            # Validate migration
            logger.info("🔍 Validating migration...")
            local_conn = self.get_local_connection()
            cursor = local_conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM statcast")
            final_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT 
                    MIN(game_date) as earliest_date,
                    MAX(game_date) as latest_date,
                    COUNT(DISTINCT game_pk) as unique_games
                FROM statcast
            """)
            
            final_stats = cursor.fetchone()
            
            cursor.close()
            local_conn.close()
            
            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("🎉 MIGRATION COMPLETED!")
            logger.info("=" * 60)
            logger.info(f"📊 Source records: {source_stats['total_records']:,}")
            logger.info(f"📊 Migrated records: {final_count:,}")
            logger.info(f"📊 Success rate: {(final_count/source_stats['total_records']*100):.1f}%")
            logger.info(f"📅 Date range: {final_stats[0]} to {final_stats[1]}")
            logger.info(f"🎮 Unique games: {final_stats[2]:,}")
            logger.info(f"⏱️ Duration: {duration}")
            logger.info(f"⚡ Rate: {final_count/duration.total_seconds():.0f} records/second")
            
            return final_count == source_stats['total_records']
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False

def main():
    """Run Statcast data migration"""
    migrator = StatcastMigrator()
    
    print("🔄 Statcast Data Migration")
    print("=" * 50)
    print("Migrating all Statcast data from Digital Ocean to local database...")
    print()
    
    success = migrator.migrate_all_statcast()
    
    if success:
        print("\n✅ Migration completed successfully!")
        return 0
    else:
        print("\n❌ Migration failed!")
        return 1

if __name__ == "__main__":
    exit(main())
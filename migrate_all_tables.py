#!/usr/bin/env python3
"""
Complete Database Migration
===========================

Migrates all important tables from Digital Ocean to local enhanced database.
"""

import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompleteDatabaseMigrator:
    """Handles migration of all tables from Digital Ocean"""
    
    def __init__(self):
        self.do_conn_params = {
            'host': os.getenv('DO_DB_HOST', 'baseball-db-do-user-23895569-0.j.db.ondigitalocean.com'),
            'port': int(os.getenv('DO_DB_PORT', 25060)),
            'user': os.getenv('DO_DB_USER', 'doadmin'),
            'password': os.getenv('DO_DB_PASSWORD'),
            'database': os.getenv('DO_DB_NAME', 'defaultdb'),
            'sslmode': 'require'
        }
        
        self.local_conn_params = {
            'host': 'localhost',
            'port': 5439,
            'user': 'mlb_user',
            'password': 'mlb_secure_pass_2024',
            'database': 'mlb_data'
        }
        
        # Tables to migrate (prioritized by importance)
        self.tables_to_migrate = [
            # Core FanGraphs data
            'fangraphs_batting',
            'fangraphs_pitching', 
            'fangraphs_fielding',
            'fangraphs_team_batting',
            'fangraphs_team_pitching',
            
            # Game and player data
            'mlb_games',
            'daily_games',
            'daily_lineups',
            'player_lookup',
            'team_rosters',
            
            # Weather and stadium data
            'game_weather',
            'stadium_weather',
        ]
    
    def analyze_source_tables(self):
        """Analyze all source tables in Digital Ocean"""
        logger.info("📊 Analyzing source tables...")
        
        try:
            do_conn = psycopg2.connect(**self.do_conn_params)
            cursor = do_conn.cursor(cursor_factory=RealDictCursor)
            
            table_stats = {}
            
            for table in self.tables_to_migrate:
                try:
                    # Get record count
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                    count = cursor.fetchone()['count']
                    
                    # Get column count
                    cursor.execute("""
                        SELECT COUNT(*) as columns 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                    """, (table,))
                    columns = cursor.fetchone()['columns']
                    
                    table_stats[table] = {
                        'records': count,
                        'columns': columns
                    }
                    
                    logger.info(f"  {table}: {count:,} records, {columns} columns")
                    
                except Exception as e:
                    logger.warning(f"  {table}: Error analyzing - {e}")
                    table_stats[table] = {'records': 0, 'columns': 0}
            
            cursor.close()
            do_conn.close()
            
            return table_stats
            
        except Exception as e:
            logger.error(f"Failed to analyze source tables: {e}")
            return {}
    
    def check_local_table_exists(self, table_name):
        """Check if table exists in local database"""
        try:
            local_conn = psycopg2.connect(**self.local_conn_params)
            cursor = local_conn.cursor()
            
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            exists = cursor.fetchone()[0]
            
            cursor.close()
            local_conn.close()
            
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if {table_name} exists: {e}")
            return False
    
    def migrate_table_schema_and_data(self, table_name):
        """Migrate both schema and data for a table using pg_dump"""
        logger.info(f"🔄 Migrating {table_name}...")
        
        try:
            # Step 1: Export schema and data from Digital Ocean
            dump_file = f"{table_name}_migration.sql"
            
            dump_command = [
                'pg_dump',
                '-h', self.do_conn_params['host'],
                '-p', str(self.do_conn_params['port']),
                '-U', self.do_conn_params['user'],
                '-d', self.do_conn_params['database'],
                '--table', table_name,
                '--file', dump_file
            ]
            
            env = {'PGPASSWORD': self.do_conn_params['password']}
            env_local = {'PGPASSWORD': self.local_conn_params['password']}
            
            result = subprocess.run(dump_command, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"  ❌ pg_dump failed for {table_name}: {result.stderr}")
                return False
            
            # Step 2: Check if we need to drop existing table
            if self.check_local_table_exists(table_name):
                logger.info(f"  🗑️ Dropping existing {table_name} table...")
                
                drop_command = [
                    'psql',
                    '-h', self.local_conn_params['host'],
                    '-p', str(self.local_conn_params['port']),
                    '-U', self.local_conn_params['user'],
                    '-d', self.local_conn_params['database'],
                    '-c', f'DROP TABLE IF EXISTS {table_name} CASCADE'
                ]
                
                result = subprocess.run(drop_command, env=env_local, capture_output=True, text=True)
                
                if result.returncode != 0:
                    logger.warning(f"  ⚠️ Could not drop {table_name}: {result.stderr}")
            
            # Step 3: Import schema and data to local database
            import_command = [
                'psql',
                '-h', self.local_conn_params['host'],
                '-p', str(self.local_conn_params['port']),
                '-U', self.local_conn_params['user'],
                '-d', self.local_conn_params['database'],
                '-f', dump_file
            ]
            
            result = subprocess.run(import_command, env=env_local, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"  ❌ Import failed for {table_name}: {result.stderr}")
                return False
            
            # Step 4: Validate migration
            local_conn = psycopg2.connect(**self.local_conn_params)
            cursor = local_conn.cursor()
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            local_count = cursor.fetchone()[0]
            
            cursor.close()
            local_conn.close()
            
            logger.info(f"  ✅ {table_name}: {local_count:,} records migrated")
            
            # Clean up dump file
            if os.path.exists(dump_file):
                os.remove(dump_file)
            
            return True
            
        except Exception as e:
            logger.error(f"  ❌ Migration failed for {table_name}: {e}")
            return False
    
    def migrate_all_tables(self):
        """Migrate all important tables from Digital Ocean"""
        logger.info("🚀 Starting complete database migration...")
        start_time = datetime.now()
        
        # Analyze source data
        source_stats = self.analyze_source_tables()
        
        migration_results = {}
        total_records_migrated = 0
        
        for table in self.tables_to_migrate:
            if table in source_stats and source_stats[table]['records'] > 0:
                success = self.migrate_table_schema_and_data(table)
                migration_results[table] = success
                
                if success:
                    total_records_migrated += source_stats[table]['records']
            else:
                logger.info(f"⏭️ Skipping {table} (no data)")
                migration_results[table] = True  # Skip empty tables
        
        # Final validation
        logger.info("🔍 Final validation...")
        
        try:
            local_conn = psycopg2.connect(**self.local_conn_params)
            cursor = local_conn.cursor(cursor_factory=RealDictCursor)
            
            # Get final table counts
            cursor.execute("""
                SELECT 
                    table_name,
                    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns
                FROM information_schema.tables t
                WHERE table_schema = 'public' 
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            final_tables = cursor.fetchall()
            
            logger.info("📋 Final database structure:")
            total_local_records = 0
            
            for table_info in final_tables:
                table_name = table_info['table_name']
                columns = table_info['columns']
                
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                total_local_records += count
                
                logger.info(f"  {table_name}: {count:,} records, {columns} columns")
            
            cursor.close()
            local_conn.close()
            
            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            successful_migrations = sum(1 for success in migration_results.values() if success)
            total_migrations = len(migration_results)
            
            logger.info("🎉 COMPLETE MIGRATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"📊 Tables migrated: {successful_migrations}/{total_migrations}")
            logger.info(f"📊 Total records in database: {total_local_records:,}")
            logger.info(f"⏱️ Duration: {duration}")
            logger.info(f"🎯 Success rate: {successful_migrations/total_migrations*100:.1f}%")
            
            return successful_migrations == total_migrations
            
        except Exception as e:
            logger.error(f"Final validation failed: {e}")
            return False

def main():
    """Run complete database migration"""
    migrator = CompleteDatabaseMigrator()
    
    print("🔄 Complete Database Migration")
    print("=" * 50)
    print("Migrating all important tables from Digital Ocean to local database...")
    print()
    
    success = migrator.migrate_all_tables()
    
    if success:
        print("\n✅ Complete migration successful!")
        return 0
    else:
        print("\n❌ Some migrations failed!")
        return 1

if __name__ == "__main__":
    exit(main())
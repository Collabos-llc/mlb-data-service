#!/usr/bin/env python3
"""
Automated Data Cleanup and Optimization Module
==============================================

Provides automated cleanup processes, retention policies, and database optimization
for production MLB data operations.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os
import shutil
import glob
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class CleanupResult:
    """Result of a cleanup operation"""
    operation: str
    table_name: Optional[str]
    records_processed: int
    records_deleted: int
    space_freed_mb: Optional[float]
    duration_seconds: float
    success: bool
    error_message: Optional[str] = None

@dataclass
class RetentionPolicy:
    """Data retention policy configuration"""
    table_name: str
    retention_days: int
    date_column: str
    partition_column: Optional[str] = None
    min_records_to_keep: int = 1000
    cascade_deletes: bool = False

class AutomatedCleanup:
    """Handles automated data cleanup with configurable retention policies"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5439/mlb_data')
        
        # Default retention policies for MLB data
        self.retention_policies = {
            'statcast_data': RetentionPolicy(
                table_name='statcast_data',
                retention_days=365,  # Keep 1 year of detailed Statcast data
                date_column='game_date',
                min_records_to_keep=50000
            ),
            'fangraphs_batting': RetentionPolicy(
                table_name='fangraphs_batting',
                retention_days=1095,  # Keep 3 years of season data
                date_column='collected_at',
                min_records_to_keep=1000
            ),
            'fangraphs_pitching': RetentionPolicy(
                table_name='fangraphs_pitching',
                retention_days=1095,  # Keep 3 years of season data
                date_column='collected_at',
                min_records_to_keep=1000
            ),
            'mlb_games': RetentionPolicy(
                table_name='mlb_games',
                retention_days=730,  # Keep 2 years of game data
                date_column='date',
                min_records_to_keep=500
            ),
            'weather_data': RetentionPolicy(
                table_name='weather_data',
                retention_days=180,  # Keep 6 months of weather data
                date_column='collected_at',
                min_records_to_keep=1000
            ),
            'player_data': RetentionPolicy(
                table_name='player_data',
                retention_days=1095,  # Keep 3 years of player data
                date_column='collected_at',
                min_records_to_keep=500
            )
        }
        
        # Log cleanup configuration
        self.log_retention_days = 30
        self.max_log_files = 100
        
        logger.info("Automated Cleanup initialized with retention policies")
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.database_url)
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists in database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table_name,))
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking table existence for {table_name}: {e}")
            return False
    
    def get_table_size_mb(self, table_name: str) -> float:
        """Get table size in MB"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT pg_total_relation_size(%s) / (1024 * 1024.0) as size_mb
                """, (table_name,))
                result = cursor.fetchone()
                return result[0] if result else 0.0
        except Exception as e:
            logger.error(f"Error getting table size for {table_name}: {e}")
            return 0.0
    
    def apply_retention_policy(self, policy: RetentionPolicy) -> CleanupResult:
        """Apply retention policy to a table"""
        start_time = datetime.now()
        operation = f"retention_cleanup_{policy.table_name}"
        
        try:
            # Check if table exists
            if not self.check_table_exists(policy.table_name):
                return CleanupResult(
                    operation=operation,
                    table_name=policy.table_name,
                    records_processed=0,
                    records_deleted=0,
                    space_freed_mb=0.0,
                    duration_seconds=0.0,
                    success=False,
                    error_message=f"Table {policy.table_name} does not exist"
                )
            
            # Get initial table size
            initial_size_mb = self.get_table_size_mb(policy.table_name)
            
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Calculate retention cutoff date
                cutoff_date = datetime.now() - timedelta(days=policy.retention_days)
                
                # Count total records
                cursor.execute(f"SELECT COUNT(*) as total FROM {policy.table_name}")
                total_records = cursor.fetchone()['total']
                
                # Count records to be deleted
                cursor.execute(f"""
                    SELECT COUNT(*) as to_delete
                    FROM {policy.table_name}
                    WHERE {policy.date_column} < %s
                """, (cutoff_date,))
                
                records_to_delete = cursor.fetchone()['to_delete']
                records_remaining = total_records - records_to_delete
                
                # Check minimum records constraint
                if records_remaining < policy.min_records_to_keep:
                    adjusted_cutoff = cutoff_date - timedelta(days=30)  # Keep more data
                    cursor.execute(f"""
                        SELECT COUNT(*) as to_delete_adjusted
                        FROM {policy.table_name}
                        WHERE {policy.date_column} < %s
                    """, (adjusted_cutoff,))
                    
                    records_to_delete = cursor.fetchone()['to_delete_adjusted']
                    cutoff_date = adjusted_cutoff
                    
                    logger.warning(f"Adjusted retention cutoff for {policy.table_name} to maintain minimum records")
                
                # Perform deletion if there are records to delete
                deleted_records = 0
                if records_to_delete > 0:
                    cursor.execute(f"""
                        DELETE FROM {policy.table_name}
                        WHERE {policy.date_column} < %s
                    """, (cutoff_date,))
                    
                    deleted_records = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"Deleted {deleted_records} old records from {policy.table_name}")
                
                # Calculate space freed
                final_size_mb = self.get_table_size_mb(policy.table_name)
                space_freed_mb = max(0.0, initial_size_mb - final_size_mb)
                
                duration = (datetime.now() - start_time).total_seconds()
                
                return CleanupResult(
                    operation=operation,
                    table_name=policy.table_name,
                    records_processed=total_records,
                    records_deleted=deleted_records,
                    space_freed_mb=space_freed_mb,
                    duration_seconds=duration,
                    success=True
                )
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Retention cleanup failed for {policy.table_name}: {e}")
            
            return CleanupResult(
                operation=operation,
                table_name=policy.table_name,
                records_processed=0,
                records_deleted=0,
                space_freed_mb=0.0,
                duration_seconds=duration,
                success=False,
                error_message=str(e)
            )
    
    def cleanup_duplicate_records(self, table_name: str, unique_columns: List[str]) -> CleanupResult:
        """Remove duplicate records based on specified columns"""
        start_time = datetime.now()
        operation = f"duplicate_cleanup_{table_name}"
        
        try:
            if not self.check_table_exists(table_name):
                return CleanupResult(
                    operation=operation,
                    table_name=table_name,
                    records_processed=0,
                    records_deleted=0,
                    space_freed_mb=0.0,
                    duration_seconds=0.0,
                    success=False,
                    error_message=f"Table {table_name} does not exist"
                )
            
            initial_size_mb = self.get_table_size_mb(table_name)
            
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Count total records
                cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
                total_records = cursor.fetchone()['total']
                
                # Build unique columns string for GROUP BY
                columns_str = ', '.join(unique_columns)
                
                # Find duplicates
                cursor.execute(f"""
                    SELECT {columns_str}, COUNT(*) as duplicate_count
                    FROM {table_name}
                    GROUP BY {columns_str}
                    HAVING COUNT(*) > 1
                """)
                
                duplicates = cursor.fetchall()
                
                total_duplicates_to_remove = 0
                for dup in duplicates:
                    total_duplicates_to_remove += dup['duplicate_count'] - 1
                
                # Remove duplicates (keep the latest by ID or collected_at)
                deleted_records = 0
                if total_duplicates_to_remove > 0:
                    # Create a temporary table with unique records
                    temp_table = f"temp_{table_name}_unique"
                    
                    # Determine row ranking strategy
                    order_column = 'collected_at' if 'collected_at' in unique_columns else 'id'
                    
                    cursor.execute(f"""
                        CREATE TEMP TABLE {temp_table} AS
                        SELECT DISTINCT ON ({columns_str}) *
                        FROM {table_name}
                        ORDER BY {columns_str}, {order_column} DESC
                    """)
                    
                    # Delete all records from original table
                    cursor.execute(f"DELETE FROM {table_name}")
                    
                    # Insert unique records back
                    cursor.execute(f"""
                        INSERT INTO {table_name}
                        SELECT * FROM {temp_table}
                    """)
                    
                    deleted_records = total_duplicates_to_remove
                    conn.commit()
                    
                    logger.info(f"Removed {deleted_records} duplicate records from {table_name}")
                
                final_size_mb = self.get_table_size_mb(table_name)
                space_freed_mb = max(0.0, initial_size_mb - final_size_mb)
                duration = (datetime.now() - start_time).total_seconds()
                
                return CleanupResult(
                    operation=operation,
                    table_name=table_name,
                    records_processed=total_records,
                    records_deleted=deleted_records,
                    space_freed_mb=space_freed_mb,
                    duration_seconds=duration,
                    success=True
                )
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Duplicate cleanup failed for {table_name}: {e}")
            
            return CleanupResult(
                operation=operation,
                table_name=table_name,
                records_processed=0,
                records_deleted=0,
                space_freed_mb=0.0,
                duration_seconds=duration,
                success=False,
                error_message=str(e)
            )
    
    def vacuum_and_analyze_tables(self) -> List[CleanupResult]:
        """Run VACUUM and ANALYZE on all monitored tables"""
        results = []
        
        for table_name in self.retention_policies.keys():
            start_time = datetime.now()
            operation = f"vacuum_analyze_{table_name}"
            
            try:
                if not self.check_table_exists(table_name):
                    continue
                
                initial_size_mb = self.get_table_size_mb(table_name)
                
                with self.get_connection() as conn:
                    conn.autocommit = True
                    cursor = conn.cursor()
                    
                    # VACUUM to reclaim space
                    cursor.execute(f"VACUUM {table_name}")
                    
                    # ANALYZE to update statistics
                    cursor.execute(f"ANALYZE {table_name}")
                    
                    final_size_mb = self.get_table_size_mb(table_name)
                    space_freed_mb = max(0.0, initial_size_mb - final_size_mb)
                    duration = (datetime.now() - start_time).total_seconds()
                    
                    results.append(CleanupResult(
                        operation=operation,
                        table_name=table_name,
                        records_processed=0,  # VACUUM doesn't delete records
                        records_deleted=0,
                        space_freed_mb=space_freed_mb,
                        duration_seconds=duration,
                        success=True
                    ))
                    
                    logger.info(f"VACUUM ANALYZE completed for {table_name}")
                
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(f"VACUUM ANALYZE failed for {table_name}: {e}")
                
                results.append(CleanupResult(
                    operation=operation,
                    table_name=table_name,
                    records_processed=0,
                    records_deleted=0,
                    space_freed_mb=0.0,
                    duration_seconds=duration,
                    success=False,
                    error_message=str(e)
                ))
        
        return results
    
    def cleanup_log_files(self, log_directory: str = None) -> CleanupResult:
        """Clean up old log files"""
        start_time = datetime.now()
        operation = "log_cleanup"
        
        log_dir = log_directory or os.path.join(os.path.dirname(__file__), '../../logs')
        
        try:
            if not os.path.exists(log_dir):
                return CleanupResult(
                    operation=operation,
                    table_name=None,
                    records_processed=0,
                    records_deleted=0,
                    space_freed_mb=0.0,
                    duration_seconds=0.0,
                    success=True,
                    error_message="Log directory does not exist"
                )
            
            # Get all log files
            log_files = glob.glob(os.path.join(log_dir, '*.log*'))
            
            cutoff_date = datetime.now() - timedelta(days=self.log_retention_days)
            files_deleted = 0
            space_freed_bytes = 0
            
            for log_file in log_files:
                try:
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                    
                    if file_mtime < cutoff_date:
                        file_size = os.path.getsize(log_file)
                        os.remove(log_file)
                        files_deleted += 1
                        space_freed_bytes += file_size
                        
                        logger.debug(f"Deleted old log file: {log_file}")
                
                except Exception as e:
                    logger.warning(f"Failed to delete log file {log_file}: {e}")
            
            # Also limit total number of log files
            remaining_files = glob.glob(os.path.join(log_dir, '*.log*'))
            
            if len(remaining_files) > self.max_log_files:
                # Sort by modification time and remove oldest
                remaining_files.sort(key=lambda f: os.path.getmtime(f))
                excess_files = remaining_files[:-self.max_log_files]
                
                for log_file in excess_files:
                    try:
                        file_size = os.path.getsize(log_file)
                        os.remove(log_file)
                        files_deleted += 1
                        space_freed_bytes += file_size
                    except Exception as e:
                        logger.warning(f"Failed to delete excess log file {log_file}: {e}")
            
            space_freed_mb = space_freed_bytes / (1024 * 1024)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Log cleanup completed: {files_deleted} files deleted, {space_freed_mb:.2f} MB freed")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=len(log_files),
                records_deleted=files_deleted,
                space_freed_mb=space_freed_mb,
                duration_seconds=duration,
                success=True
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Log cleanup failed: {e}")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=0,
                records_deleted=0,
                space_freed_mb=0.0,
                duration_seconds=duration,
                success=False,
                error_message=str(e)
            )
    
    def run_full_cleanup(self) -> Dict[str, Any]:
        """Run complete cleanup routine"""
        start_time = datetime.now()
        logger.info("Starting full automated cleanup routine")
        
        results = {
            'started_at': start_time.isoformat(),
            'retention_cleanup': [],
            'duplicate_cleanup': [],
            'vacuum_results': [],
            'log_cleanup': None,
            'summary': {
                'total_operations': 0,
                'successful_operations': 0,
                'failed_operations': 0,
                'total_records_deleted': 0,
                'total_space_freed_mb': 0.0,
                'total_duration_seconds': 0.0
            }
        }
        
        # Apply retention policies
        for policy in self.retention_policies.values():
            result = self.apply_retention_policy(policy)
            results['retention_cleanup'].append(asdict(result))
            
            if result.success:
                results['summary']['successful_operations'] += 1
            else:
                results['summary']['failed_operations'] += 1
            
            results['summary']['total_records_deleted'] += result.records_deleted
            results['summary']['total_space_freed_mb'] += result.space_freed_mb or 0.0
        
        # Clean up duplicates in key tables
        duplicate_cleanup_configs = [
            ('statcast_data', ['player_name', 'game_date', 'pitch_type', 'release_speed']),
            ('fangraphs_batting', ['Name', 'Team', 'Season']),
            ('fangraphs_pitching', ['Name', 'Team', 'Season'])
        ]
        
        for table_name, unique_columns in duplicate_cleanup_configs:
            if self.check_table_exists(table_name):
                result = self.cleanup_duplicate_records(table_name, unique_columns)
                results['duplicate_cleanup'].append(asdict(result))
                
                if result.success:
                    results['summary']['successful_operations'] += 1
                else:
                    results['summary']['failed_operations'] += 1
                
                results['summary']['total_records_deleted'] += result.records_deleted
                results['summary']['total_space_freed_mb'] += result.space_freed_mb or 0.0
        
        # VACUUM and ANALYZE tables
        vacuum_results = self.vacuum_and_analyze_tables()
        results['vacuum_results'] = [asdict(result) for result in vacuum_results]
        
        for result in vacuum_results:
            if result.success:
                results['summary']['successful_operations'] += 1
            else:
                results['summary']['failed_operations'] += 1
            
            results['summary']['total_space_freed_mb'] += result.space_freed_mb or 0.0
        
        # Clean up log files
        log_result = self.cleanup_log_files()
        results['log_cleanup'] = asdict(log_result)
        
        if log_result.success:
            results['summary']['successful_operations'] += 1
        else:
            results['summary']['failed_operations'] += 1
        
        results['summary']['total_space_freed_mb'] += log_result.space_freed_mb or 0.0
        
        # Calculate totals
        results['summary']['total_operations'] = (
            results['summary']['successful_operations'] + 
            results['summary']['failed_operations']
        )
        
        end_time = datetime.now()
        results['completed_at'] = end_time.isoformat()
        results['summary']['total_duration_seconds'] = (end_time - start_time).total_seconds()
        
        logger.info(f"Full cleanup completed: {results['summary']['successful_operations']}/{results['summary']['total_operations']} operations successful")
        logger.info(f"Total space freed: {results['summary']['total_space_freed_mb']:.2f} MB")
        
        return results

class MemoryAndCacheCleanup:
    """Handles memory and cache cleanup operations"""
    
    def __init__(self):
        self.temp_dirs = ['/tmp', '/var/tmp']
        logger.info("Memory and Cache Cleanup initialized")
    
    def clear_pybaseball_cache(self) -> CleanupResult:
        """Clear PyBaseball cache to ensure fresh data"""
        start_time = datetime.now()
        operation = "pybaseball_cache_cleanup"
        
        try:
            import pybaseball as pyb
            
            # Clear cache
            pyb.cache.purge()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("PyBaseball cache cleared")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=1,
                records_deleted=1,
                space_freed_mb=None,  # Unknown space freed
                duration_seconds=duration,
                success=True
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"PyBaseball cache cleanup failed: {e}")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=0,
                records_deleted=0,
                space_freed_mb=0.0,
                duration_seconds=duration,
                success=False,
                error_message=str(e)
            )
    
    def cleanup_temp_files(self, max_age_hours: int = 24) -> CleanupResult:
        """Clean up temporary files older than specified age"""
        start_time = datetime.now()
        operation = "temp_files_cleanup"
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            files_deleted = 0
            space_freed_bytes = 0
            
            for temp_dir in self.temp_dirs:
                if not os.path.exists(temp_dir):
                    continue
                
                try:
                    # Find temporary files related to our application
                    temp_patterns = [
                        os.path.join(temp_dir, 'mlb_*'),
                        os.path.join(temp_dir, 'statcast_*'),
                        os.path.join(temp_dir, 'fangraphs_*'),
                        os.path.join(temp_dir, '*.tmp')
                    ]
                    
                    for pattern in temp_patterns:
                        temp_files = glob.glob(pattern)
                        
                        for temp_file in temp_files:
                            try:
                                file_mtime = datetime.fromtimestamp(os.path.getmtime(temp_file))
                                
                                if file_mtime < cutoff_time:
                                    if os.path.isfile(temp_file):
                                        file_size = os.path.getsize(temp_file)
                                        os.remove(temp_file)
                                        files_deleted += 1
                                        space_freed_bytes += file_size
                                    elif os.path.isdir(temp_file):
                                        dir_size = sum(
                                            os.path.getsize(os.path.join(dirpath, filename))
                                            for dirpath, dirnames, filenames in os.walk(temp_file)
                                            for filename in filenames
                                        )
                                        shutil.rmtree(temp_file)
                                        files_deleted += 1
                                        space_freed_bytes += dir_size
                            
                            except Exception as e:
                                logger.warning(f"Failed to delete temp file {temp_file}: {e}")
                
                except Exception as e:
                    logger.warning(f"Error cleaning temp directory {temp_dir}: {e}")
            
            space_freed_mb = space_freed_bytes / (1024 * 1024)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Temp files cleanup: {files_deleted} files/dirs deleted, {space_freed_mb:.2f} MB freed")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=files_deleted,
                records_deleted=files_deleted,
                space_freed_mb=space_freed_mb,
                duration_seconds=duration,
                success=True
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Temp files cleanup failed: {e}")
            
            return CleanupResult(
                operation=operation,
                table_name=None,
                records_processed=0,
                records_deleted=0,
                space_freed_mb=0.0,
                duration_seconds=duration,
                success=False,
                error_message=str(e)
            )

# Convenience functions for scheduled cleanup
def run_daily_cleanup() -> Dict[str, Any]:
    """Run daily cleanup routine"""
    cleanup = AutomatedCleanup()
    return cleanup.run_full_cleanup()

def run_cache_cleanup() -> List[CleanupResult]:
    """Run cache and memory cleanup"""
    cache_cleanup = MemoryAndCacheCleanup()
    
    results = [
        cache_cleanup.clear_pybaseball_cache(),
        cache_cleanup.cleanup_temp_files()
    ]
    
    return results

def get_cleanup_status() -> Dict[str, Any]:
    """Get status of cleanup operations"""
    cleanup = AutomatedCleanup()
    
    # Get table sizes
    table_sizes = {}
    for table_name in cleanup.retention_policies.keys():
        if cleanup.check_table_exists(table_name):
            table_sizes[table_name] = cleanup.get_table_size_mb(table_name)
    
    return {
        'timestamp': datetime.now().isoformat(),
        'monitored_tables': list(cleanup.retention_policies.keys()),
        'table_sizes_mb': table_sizes,
        'total_database_size_mb': sum(table_sizes.values()),
        'retention_policies': {
            name: asdict(policy) for name, policy in cleanup.retention_policies.items()
        }
    }
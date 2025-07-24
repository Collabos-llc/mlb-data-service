#!/usr/bin/env python3
"""
Enhanced MLB Database Manager
============================

Handles comprehensive FanGraphs and Statcast data with full schema support.
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pandas as pd
import pybaseball as pyb
import time

logger = logging.getLogger(__name__)

# Import monitoring capabilities
try:
    from .monitoring.data_monitor import DataFreshnessTracker
    MONITORING_AVAILABLE = True
except ImportError:
    MONITORING_AVAILABLE = False
    logger.warning("Data monitoring not available in enhanced database")

class EnhancedDatabaseManager:
    """Enhanced database manager for comprehensive MLB data"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5439/mlb_data')
        self.pool = None
        self._init_connection_pool()
        
        # Disable PyBaseball cache for fresh data
        pyb.cache.disable()
        
        # Initialize monitoring
        self.performance_metrics = {
            'operations_count': 0,
            'total_records_processed': 0,
            'average_operation_time': 0.0,
            'last_operation_time': None
        }
        
        # Initialize freshness tracker if available
        self.freshness_tracker = None
        if MONITORING_AVAILABLE:
            try:
                self.freshness_tracker = DataFreshnessTracker()
                logger.info("Database freshness tracker initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize freshness tracker: {e}")
    
    def _init_connection_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=self.database_url
            )
            logger.info("Enhanced database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize enhanced database pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self.pool:
            return self.pool.getconn()
        return None
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool and conn:
            self.pool.putconn(conn)
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                self.return_connection(conn)
                return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def _track_operation_performance(self, operation_name: str, start_time: float, records_processed: int) -> None:
        """Track performance metrics for database operations"""
        operation_time = time.time() - start_time
        
        self.performance_metrics['operations_count'] += 1
        self.performance_metrics['total_records_processed'] += records_processed
        self.performance_metrics['last_operation_time'] = operation_time
        
        # Calculate running average
        total_ops = self.performance_metrics['operations_count']
        current_avg = self.performance_metrics['average_operation_time']
        self.performance_metrics['average_operation_time'] = (
            (current_avg * (total_ops - 1) + operation_time) / total_ops
        )
        
        logger.debug(f"Operation {operation_name}: {operation_time:.2f}s, {records_processed} records")
    
    def _validate_data_before_storage(self, data: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Validate data quality before storing in database"""
        validation_result = {
            'table_name': table_name,
            'total_records': len(data),
            'validation_passed': True,
            'issues': [],
            'null_percentages': {},
            'duplicate_records': 0
        }
        
        if data.empty:
            validation_result['validation_passed'] = False
            validation_result['issues'].append('DataFrame is empty')
            return validation_result
        
        # Check for null percentages
        for column in data.columns:
            null_count = data[column].isnull().sum()
            null_percentage = null_count / len(data)
            validation_result['null_percentages'][column] = round(null_percentage, 4)
            
            if null_percentage > 0.5:  # More than 50% nulls
                validation_result['issues'].append(f"High null percentage in {column}: {null_percentage:.2%}")
        
        # Check for duplicates (if possible)
        try:
            if 'Name' in data.columns and 'Team' in data.columns:
                duplicates = data.duplicated(subset=['Name', 'Team']).sum()
                validation_result['duplicate_records'] = duplicates
                
                if duplicates > 0:
                    validation_result['issues'].append(f"Found {duplicates} duplicate records")
        except Exception as e:
            logger.debug(f"Could not check duplicates for {table_name}: {e}")
        
        # Set validation status
        if validation_result['issues']:
            validation_result['validation_passed'] = False
        
        return validation_result
    
    def collect_and_store_fangraphs_batting(self, season: int = None, min_pa: int = 10) -> int:
        """Collect and store comprehensive FanGraphs batting data"""
        start_time = time.time()
        operation_name = f"fangraphs_batting_{season}"
        
        if season is None:
            season = datetime.now().year
            
        logger.info(f"Collecting FanGraphs batting data for {season} (min {min_pa} PA)")
        
        try:
            # Collect data from FanGraphs via PyBaseball
            batting_data = pyb.batting_stats(season, qual=min_pa, ind=1)
            
            if batting_data is None or batting_data.empty:
                logger.warning(f"No FanGraphs batting data returned for {season}")
                self._track_operation_performance(operation_name, start_time, 0)
                return 0
            
            logger.info(f"Retrieved {len(batting_data)} batting records from FanGraphs")
            
            # Validate data quality before storage
            validation_result = self._validate_data_before_storage(batting_data, 'fangraphs_batting')
            
            if not validation_result['validation_passed']:
                logger.warning(f"Data quality issues detected: {validation_result['issues']}")
            else:
                logger.info(f"Data validation passed for {len(batting_data)} records")
            
            # Store in database using direct DataFrame approach
            conn = None
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Clear existing data for the season
                cursor.execute('DELETE FROM fangraphs_batting WHERE "Season" = %s', (season,))
                
                # Insert new data using pandas to_sql method
                batting_data['Season'] = season
                
                # Use pandas built-in database insertion
                import sqlalchemy
                from sqlalchemy import create_engine
                
                # Create engine from our connection URL
                engine = create_engine(self.database_url)
                
                # Use pandas to_sql for direct insertion
                batting_data.to_sql(
                    'fangraphs_batting', 
                    engine, 
                    if_exists='append', 
                    index=False,
                    method='multi'
                )
                
                conn.commit()
                cursor.close()
                
                # Track performance and log success
                records_stored = len(batting_data)
                self._track_operation_performance(operation_name, start_time, records_stored)
                
                logger.info(f"Stored {records_stored} FanGraphs batting records for {season}")
                return records_stored
                
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Failed to store FanGraphs batting data: {e}")
                self._track_operation_performance(operation_name, start_time, 0)
                return 0
            finally:
                if conn:
                    self.return_connection(conn)
                    
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs batting data: {e}")
            self._track_operation_performance(operation_name, start_time, 0)
            return 0
    
    def collect_and_store_fangraphs_pitching(self, season: int = None, min_ip: int = 5) -> int:
        """Collect and store comprehensive FanGraphs pitching data"""
        if season is None:
            season = datetime.now().year
            
        logger.info(f"Collecting FanGraphs pitching data for {season} (min {min_ip} IP)")
        
        try:
            # Collect data from FanGraphs via PyBaseball
            pitching_data = pyb.pitching_stats(season, qual=min_ip, ind=1)
            
            if pitching_data is None or pitching_data.empty:
                logger.warning(f"No FanGraphs pitching data returned for {season}")
                return 0
            
            logger.info(f"Retrieved {len(pitching_data)} pitching records from FanGraphs")
            
            # Store in database
            conn = None
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Clear existing data for the season
                cursor.execute('DELETE FROM fangraphs_pitching WHERE "Season" = %s', (season,))
                
                # Insert new data
                pitching_data['Season'] = season
                
                # Use pandas to_sql equivalent with psycopg2
                columns = list(pitching_data.columns)
                
                # Create values list for bulk insert
                values = []
                for _, row in pitching_data.iterrows():
                    row_values = []
                    for col in columns:
                        value = row[col]
                        if pd.isna(value):
                            row_values.append(None)
                        else:
                            row_values.append(value)
                    values.append(tuple(row_values))
                
                # Build insert query - escape quotes properly  
                column_names = ['"' + col + '"' for col in columns]
                placeholders = ','.join(['%s'] * len(columns))
                insert_query = """
                    INSERT INTO fangraphs_pitching ({})
                    VALUES ({})
                    ON CONFLICT ("IDfg", "Season") DO UPDATE SET
                    "Name" = EXCLUDED."Name",
                    "Team" = EXCLUDED."Team",
                    "G" = EXCLUDED."G",
                    "IP" = EXCLUDED."IP",
                    "FIP" = EXCLUDED."FIP",
                    "xFIP" = EXCLUDED."xFIP",
                    "WAR" = EXCLUDED."WAR"
                """.format(','.join(column_names), placeholders)
                
                execute_values(cursor, insert_query, values, page_size=1000)
                
                conn.commit()
                cursor.close()
                
                logger.info(f"Stored {len(pitching_data)} FanGraphs pitching records for {season}")
                return len(pitching_data)
                
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Failed to store FanGraphs pitching data: {e}")
                return 0
            finally:
                if conn:
                    self.return_connection(conn)
                    
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs pitching data: {e}")
            return 0
    
    def collect_and_store_statcast(self, start_date: str = None, end_date: str = None) -> int:
        """Collect and store comprehensive Statcast data"""
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Collecting Statcast data from {start_date} to {end_date}")
        
        try:
            # Collect data from Statcast via PyBaseball
            statcast_data = pyb.statcast(start_dt=start_date, end_dt=end_date)
            
            if statcast_data is None or statcast_data.empty:
                logger.warning(f"No Statcast data returned for {start_date} to {end_date}")
                return 0
            
            logger.info(f"Retrieved {len(statcast_data)} Statcast records")
            
            # Store in database
            conn = None
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Clear existing data for the date range
                cursor.execute("""
                    DELETE FROM statcast 
                    WHERE game_date >= %s AND game_date <= %s
                """, (start_date, end_date))
                
                # Use pandas to_sql equivalent with psycopg2
                columns = list(statcast_data.columns)
                
                # Create values list for bulk insert
                values = []
                for _, row in statcast_data.iterrows():
                    row_values = []
                    for col in columns:
                        value = row[col]
                        if pd.isna(value):
                            row_values.append(None)
                        else:
                            row_values.append(value)
                    values.append(tuple(row_values))
                
                # Build insert query - escape quotes properly for special column names
                column_names = ['"' + col + '"' if ' ' in col or '-' in col or col.startswith(tuple('0123456789')) else col for col in columns]
                placeholders = ','.join(['%s'] * len(columns))
                insert_query = f"""
                    INSERT INTO statcast ({','.join(column_names)})
                    VALUES ({placeholders})
                    ON CONFLICT (game_pk, at_bat_number, pitch_number) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    events = EXCLUDED.events,
                    description = EXCLUDED.description,
                    launch_speed = EXCLUDED.launch_speed,
                    launch_angle = EXCLUDED.launch_angle,
                    release_spin_rate = EXCLUDED.release_spin_rate
                """
                
                # Use executemany for more reliable insertion
                cursor.executemany(insert_query, values)
                
                conn.commit()
                cursor.close()
                
                logger.info(f"Stored {len(statcast_data)} Statcast records")
                return len(statcast_data)
                
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Failed to store Statcast data: {e}")
                return 0
            finally:
                if conn:
                    self.return_connection(conn)
                    
        except Exception as e:
            logger.error(f"Failed to collect Statcast data: {e}")
            return 0
    
    def get_fangraphs_batting_summary(self, season: int = None, limit: int = 10) -> List[Dict]:
        """Get summary of FanGraphs batting data"""
        if season is None:
            season = datetime.now().year
            
        try:
            # Use direct connection approach to avoid pool issues
            import psycopg2
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT 
                    "IDfg" as player_id,
                    "Name" as player_name,
                    "Team" as team,
                    "G" as games,
                    "PA" as plate_appearances,
                    "HR" as home_runs,
                    "wOBA" as woba,
                    "wRC+" as wrc_plus,
                    "WAR" as war,
                    "Barrel%" as barrel_percent,
                    "xwOBA" as expected_woba,
                    "EV" as exit_velocity
                FROM fangraphs_batting
                WHERE "Season" = %(season)s AND "PA" >= 10
                ORDER BY "wRC+" DESC NULLS LAST
                LIMIT %(limit)s
            """, {'season': season, 'limit': limit})
            
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get FanGraphs batting summary: {e}")
            return []
    
    def get_statcast_summary(self, limit: int = 10) -> List[Dict]:
        """Get summary of Statcast data"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT 
                    player_name,
                    game_date,
                    events,
                    launch_speed,
                    launch_angle,
                    release_spin_rate,
                    estimated_woba_using_speedangle,
                    pfx_x,
                    pfx_z,
                    plate_x,
                    plate_z
                FROM statcast
                WHERE launch_speed IS NOT NULL
                ORDER BY game_date DESC, launch_speed DESC
                LIMIT %s
            """, (limit,))
            
            results = []
            for row in cursor.fetchall():
                result = dict(row)
                # Convert datetime to string for JSON serialization
                if result.get('game_date'):
                    result['game_date'] = result['game_date'].isoformat()
                results.append(result)
            
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"Failed to get Statcast summary: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            stats = {}
            
            # FanGraphs batting count
            cursor.execute('SELECT COUNT(*) as count FROM fangraphs_batting')
            stats['fangraphs_batting_count'] = cursor.fetchone()['count']
            
            # FanGraphs pitching count
            cursor.execute('SELECT COUNT(*) as count FROM fangraphs_pitching')
            stats['fangraphs_pitching_count'] = cursor.fetchone()['count']
            
            # Statcast count
            cursor.execute('SELECT COUNT(*) as count FROM statcast')
            stats['statcast_count'] = cursor.fetchone()['count']
            
            # Latest data dates
            cursor.execute('SELECT MAX("Season") as latest_season FROM fangraphs_batting')
            result = cursor.fetchone()
            stats['latest_fangraphs_season'] = result['latest_season'] if result['latest_season'] else None
            
            cursor.execute('SELECT MAX(game_date) as latest_game FROM statcast')
            result = cursor.fetchone()
            stats['latest_statcast_date'] = result['latest_game'].isoformat() if result['latest_game'] else None
            
            cursor.close()
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_unified_player_profile(self, player_name: str = None, fangraphs_id: int = None, mlb_id: int = None) -> Dict[str, Any]:
        """Get unified player profile combining FanGraphs, Statcast, and lookup data"""
        try:
            import psycopg2
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Build WHERE clause based on provided identifier
            where_conditions = []
            params = {}
            
            if fangraphs_id:
                where_conditions.append("pl.key_fangraphs = %(fg_id)s")
                params['fg_id'] = fangraphs_id
            elif mlb_id:
                where_conditions.append("pl.key_mlbam = %(mlb_id)s")
                params['mlb_id'] = mlb_id
            elif player_name:
                where_conditions.append("(pl.name_first || ' ' || pl.name_last) ILIKE %(player_name)s")
                params['player_name'] = f"%{player_name}%"
            else:
                return {}
            
            where_clause = " AND ".join(where_conditions)
            
            cursor.execute(f"""
                SELECT 
                    pl.name_first || ' ' || pl.name_last as full_name,
                    pl.name_first,
                    pl.name_last,
                    pl.key_fangraphs,
                    pl.key_mlbam,
                    pl.key_bbref,
                    pl.key_retro,
                    pl.mlb_played_first,
                    pl.mlb_played_last,
                    
                    -- FanGraphs batting data (2025)
                    fb."Team" as current_team,
                    fb."G" as games,
                    fb."PA" as plate_appearances,
                    fb."HR" as home_runs,
                    fb."wOBA" as woba,
                    fb."wRC+" as wrc_plus,
                    fb."WAR" as war,
                    
                    -- Statcast summary
                    (SELECT COUNT(*) FROM statcast WHERE batter = pl.key_mlbam) as statcast_abs,
                    (SELECT AVG(launch_speed) FROM statcast WHERE batter = pl.key_mlbam AND launch_speed IS NOT NULL) as avg_exit_velo,
                    (SELECT AVG(launch_angle) FROM statcast WHERE batter = pl.key_mlbam AND launch_angle IS NOT NULL) as avg_launch_angle
                    
                FROM player_lookup pl
                LEFT JOIN fangraphs_batting fb ON pl.key_fangraphs = fb."IDfg" AND fb."Season" = 2025
                WHERE {where_clause}
                LIMIT 1
            """, params)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return dict(result)
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Failed to get unified player profile: {e}")
            return {}
    
    def search_players(self, query: str, limit: int = 10) -> List[Dict]:
        """Search players across all systems"""
        try:
            import psycopg2
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT 
                    pl.name_first || ' ' || pl.name_last as full_name,
                    pl.key_fangraphs,
                    pl.key_mlbam,
                    pl.key_bbref,
                    COALESCE(fb."Team", 'N/A') as current_team,
                    COALESCE(fb."PA", 0) as plate_appearances_2025,
                    (SELECT COUNT(*) FROM statcast WHERE batter = pl.key_mlbam) as statcast_abs
                FROM player_lookup pl
                LEFT JOIN fangraphs_batting fb ON pl.key_fangraphs = fb."IDfg" AND fb."Season" = 2025
                WHERE (pl.name_first || ' ' || pl.name_last) ILIKE %(query)s
                   OR pl.name_last ILIKE %(query)s
                ORDER BY 
                    CASE WHEN fb."PA" IS NOT NULL THEN 1 ELSE 2 END,
                    COALESCE(fb."PA", 0) DESC
                LIMIT %(limit)s
            """, {'query': f"%{query}%", 'limit': limit})
            
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to search players: {e}")
            return []

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for database operations"""
        return {
            'timestamp': datetime.now().isoformat(),
            'operations_completed': self.performance_metrics['operations_count'],
            'total_records_processed': self.performance_metrics['total_records_processed'],
            'average_operation_time_seconds': round(self.performance_metrics['average_operation_time'], 3),
            'last_operation_time_seconds': round(self.performance_metrics['last_operation_time'] or 0, 3),
            'connection_pool_status': self.test_connection()
        }
    
    def get_data_freshness_status(self) -> Dict[str, Any]:
        """Get comprehensive data freshness status"""
        if not self.freshness_tracker:
            return {'error': 'Freshness tracker not available'}
        
        try:
            return self.freshness_tracker.get_system_health_summary()
        except Exception as e:
            logger.error(f"Failed to get freshness status: {e}")
            return {'error': str(e)}
    
    def get_database_health_report(self) -> Dict[str, Any]:
        """Get comprehensive database health report"""
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'connection_status': 'unknown',
            'performance_metrics': {},
            'data_freshness': {},
            'table_statistics': {},
            'overall_health': 'unknown'
        }
        
        try:
            # Test connection
            health_report['connection_status'] = 'healthy' if self.test_connection() else 'failed'
            
            # Get performance metrics
            health_report['performance_metrics'] = self.get_performance_metrics()
            
            # Get data freshness
            health_report['data_freshness'] = self.get_data_freshness_status()
            
            # Get table statistics
            health_report['table_statistics'] = self.get_database_stats()
            
            # Determine overall health
            connection_ok = health_report['connection_status'] == 'healthy'
            has_recent_data = bool(health_report['table_statistics'])
            freshness_ok = not health_report['data_freshness'].get('error')
            
            if connection_ok and has_recent_data and freshness_ok:
                health_report['overall_health'] = 'healthy'
            elif connection_ok:
                health_report['overall_health'] = 'degraded'
            else:
                health_report['overall_health'] = 'critical'
            
            return health_report
            
        except Exception as e:
            logger.error(f"Failed to generate health report: {e}")
            health_report['overall_health'] = 'critical'
            health_report['error'] = str(e)
            return health_report

    def close(self):
        """Close all database connections"""
        if self.pool:
            self.pool.closeall()
            logger.info("Enhanced database connection pool closed")
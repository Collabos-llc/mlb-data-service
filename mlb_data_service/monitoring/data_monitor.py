#!/usr/bin/env python3
"""
Data Monitoring and Quality Assurance Module
===========================================

Provides real-time data freshness tracking, quality validation, and failure detection
for production MLB data operations.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import os
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class DataFreshnessLevel(Enum):
    """Data freshness levels for alerting"""
    FRESH = "fresh"
    STALE = "stale"
    CRITICAL = "critical"
    MISSING = "missing"

class DataSourceStatus(Enum):
    """Data source operational status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"

@dataclass
class DataFreshnessMetric:
    """Metric for tracking data freshness"""
    source_name: str
    table_name: str
    last_update: Optional[datetime]
    record_count: int
    freshness_threshold_hours: float
    current_freshness_level: DataFreshnessLevel
    staleness_duration_hours: Optional[float]
    next_expected_update: Optional[datetime]
    status: DataSourceStatus

@dataclass
class DataQualityIssue:
    """Represents a data quality problem"""
    source_name: str
    table_name: str
    issue_type: str
    severity: str
    description: str
    affected_records: int
    detected_at: datetime
    sample_data: Optional[Dict[str, Any]] = None

class DataFreshnessTracker:
    """Tracks data freshness across all MLB data sources"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5439/mlb_data')
        
        # Freshness thresholds for different data sources (in hours)
        self.freshness_thresholds = {
            'fangraphs_batting': 24.0,      # Daily updates expected
            'fangraphs_pitching': 24.0,     # Daily updates expected
            'statcast_data': 2.0,           # Every 2 hours during season
            'mlb_games': 0.25,              # Every 15 minutes during games
            'weather_data': 1.0,            # Hourly updates
            'player_data': 24.0             # Daily player updates
        }
        
        # Critical thresholds (when to send alerts)
        self.critical_thresholds = {
            'fangraphs_batting': 48.0,      # 2 days without update
            'fangraphs_pitching': 48.0,     # 2 days without update
            'statcast_data': 6.0,           # 6 hours without update
            'mlb_games': 1.0,               # 1 hour without game updates
            'weather_data': 4.0,            # 4 hours without weather
            'player_data': 72.0             # 3 days without player updates
        }
        
        logger.info("Data Freshness Tracker initialized")
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.database_url)
    
    def check_table_freshness(self, table_name: str, timestamp_column: str = 'collected_at') -> DataFreshnessMetric:
        """Check freshness of a specific table"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get latest record and count
                query = f"""
                SELECT 
                    MAX({timestamp_column}) as last_update,
                    COUNT(*) as record_count
                FROM {table_name}
                """
                
                cursor.execute(query)
                result = cursor.fetchone()
                
                last_update = result['last_update'] if result else None
                record_count = result['record_count'] if result else 0
                
                # Calculate freshness
                current_time = datetime.now()
                staleness_hours = None
                freshness_level = DataFreshnessLevel.MISSING
                status = DataSourceStatus.UNKNOWN
                
                if last_update:
                    if isinstance(last_update, str):
                        last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                    
                    staleness_duration = current_time - last_update.replace(tzinfo=None)
                    staleness_hours = staleness_duration.total_seconds() / 3600
                    
                    # Determine freshness level
                    threshold = self.freshness_thresholds.get(table_name, 24.0)
                    critical_threshold = self.critical_thresholds.get(table_name, 48.0)
                    
                    if staleness_hours <= threshold:
                        freshness_level = DataFreshnessLevel.FRESH
                        status = DataSourceStatus.HEALTHY
                    elif staleness_hours <= critical_threshold:
                        freshness_level = DataFreshnessLevel.STALE
                        status = DataSourceStatus.DEGRADED
                    else:
                        freshness_level = DataFreshnessLevel.CRITICAL
                        status = DataSourceStatus.FAILED
                
                # Calculate next expected update
                next_expected = None
                if last_update and table_name in self.freshness_thresholds:
                    threshold_hours = self.freshness_thresholds[table_name]
                    next_expected = last_update.replace(tzinfo=None) + timedelta(hours=threshold_hours)
                
                return DataFreshnessMetric(
                    source_name=table_name,
                    table_name=table_name,
                    last_update=last_update.replace(tzinfo=None) if last_update else None,
                    record_count=record_count,
                    freshness_threshold_hours=self.freshness_thresholds.get(table_name, 24.0),
                    current_freshness_level=freshness_level,
                    staleness_duration_hours=staleness_hours,
                    next_expected_update=next_expected,
                    status=status
                )
                
        except Exception as e:
            logger.error(f"Error checking freshness for {table_name}: {e}")
            return DataFreshnessMetric(
                source_name=table_name,
                table_name=table_name,
                last_update=None,
                record_count=0,
                freshness_threshold_hours=self.freshness_thresholds.get(table_name, 24.0),
                current_freshness_level=DataFreshnessLevel.MISSING,
                staleness_duration_hours=None,
                next_expected_update=None,
                status=DataSourceStatus.FAILED
            )
    
    def get_all_freshness_metrics(self) -> List[DataFreshnessMetric]:
        """Get freshness metrics for all monitored tables"""
        metrics = []
        
        # Standard table mappings
        table_configs = [
            ('fangraphs_batting', 'collected_at'),
            ('fangraphs_pitching', 'collected_at'),
            ('statcast_data', 'collected_at'),
            ('mlb_games', 'collected_at'),
            ('weather_data', 'collected_at'),
            ('player_data', 'collected_at')
        ]
        
        for table_name, timestamp_col in table_configs:
            try:
                metric = self.check_table_freshness(table_name, timestamp_col)
                metrics.append(metric)
            except Exception as e:
                logger.error(f"Failed to get metrics for {table_name}: {e}")
                # Add failed metric
                metrics.append(DataFreshnessMetric(
                    source_name=table_name,
                    table_name=table_name,
                    last_update=None,
                    record_count=0,
                    freshness_threshold_hours=self.freshness_thresholds.get(table_name, 24.0),
                    current_freshness_level=DataFreshnessLevel.MISSING,
                    staleness_duration_hours=None,
                    next_expected_update=None,
                    status=DataSourceStatus.FAILED
                ))
        
        return metrics
    
    def get_stale_data_sources(self) -> List[DataFreshnessMetric]:
        """Get data sources that are stale or critical"""
        all_metrics = self.get_all_freshness_metrics()
        return [
            metric for metric in all_metrics 
            if metric.current_freshness_level in [
                DataFreshnessLevel.STALE, 
                DataFreshnessLevel.CRITICAL, 
                DataFreshnessLevel.MISSING
            ]
        ]
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """Get overall system health summary"""
        metrics = self.get_all_freshness_metrics()
        
        health_counts = {
            'fresh': 0,
            'stale': 0,
            'critical': 0,
            'missing': 0
        }
        
        for metric in metrics:
            health_counts[metric.current_freshness_level.value] += 1
        
        total_sources = len(metrics)
        healthy_percentage = (health_counts['fresh'] / total_sources * 100) if total_sources > 0 else 0
        
        # Determine overall system status
        if health_counts['critical'] > 0 or health_counts['missing'] > 0:
            overall_status = DataSourceStatus.FAILED
        elif health_counts['stale'] > 0:
            overall_status = DataSourceStatus.DEGRADED
        else:
            overall_status = DataSourceStatus.HEALTHY
        
        return {
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status.value,
            'healthy_percentage': round(healthy_percentage, 2),
            'source_counts': health_counts,
            'total_sources': total_sources,
            'needs_attention': health_counts['stale'] + health_counts['critical'] + health_counts['missing'],
            'metrics': [asdict(metric) for metric in metrics]
        }

class DataQualityValidator:
    """Validates data quality and detects anomalies"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5439/mlb_data')
        
        # Quality rules for different data types
        self.quality_rules = {
            'fangraphs_batting': {
                'required_columns': ['Name', 'Team', 'G', 'PA', 'AVG'],
                'numeric_ranges': {
                    'AVG': (0.0, 1.0),
                    'OBP': (0.0, 1.0),
                    'SLG': (0.0, 4.0),
                    'wRC+': (0, 300)
                },
                'null_tolerance': 0.05  # 5% null values allowed
            },
            'statcast_data': {
                'required_columns': ['player_name', 'game_date', 'events'],
                'numeric_ranges': {
                    'release_speed': (50.0, 110.0),
                    'launch_speed': (0.0, 130.0),
                    'launch_angle': (-90.0, 90.0)
                },
                'null_tolerance': 0.10  # 10% null values allowed for optional fields
            }
        }
        
        logger.info("Data Quality Validator initialized")
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.database_url)
    
    def validate_table_schema(self, table_name: str) -> List[DataQualityIssue]:
        """Validate table schema and required columns"""
        issues = []
        
        if table_name not in self.quality_rules:
            return issues
        
        required_columns = self.quality_rules[table_name]['required_columns']
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get table columns
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                
                existing_columns = [row['column_name'] for row in cursor.fetchall()]
                missing_columns = set(required_columns) - set(existing_columns)
                
                if missing_columns:
                    issues.append(DataQualityIssue(
                        source_name=table_name,
                        table_name=table_name,
                        issue_type='missing_columns',
                        severity='critical',
                        description=f"Missing required columns: {', '.join(missing_columns)}",
                        affected_records=0,
                        detected_at=datetime.now()
                    ))
                
        except Exception as e:
            logger.error(f"Schema validation failed for {table_name}: {e}")
            issues.append(DataQualityIssue(
                source_name=table_name,
                table_name=table_name,
                issue_type='schema_check_failed',
                severity='critical',
                description=f"Schema validation failed: {str(e)}",
                affected_records=0,
                detected_at=datetime.now()
            ))
        
        return issues
    
    def validate_data_ranges(self, table_name: str) -> List[DataQualityIssue]:
        """Validate numeric data ranges"""
        issues = []
        
        if table_name not in self.quality_rules:
            return issues
        
        numeric_ranges = self.quality_rules[table_name].get('numeric_ranges', {})
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                for column, (min_val, max_val) in numeric_ranges.items():
                    # Check for out-of-range values
                    cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM {table_name}
                        WHERE {column} IS NOT NULL 
                        AND ({column} < %s OR {column} > %s)
                    """, (min_val, max_val))
                    
                    result = cursor.fetchone()
                    out_of_range_count = result['count'] if result else 0
                    
                    if out_of_range_count > 0:
                        # Get sample of out-of-range values
                        cursor.execute(f"""
                            SELECT {column}, COUNT(*) as count
                            FROM {table_name}
                            WHERE {column} IS NOT NULL 
                            AND ({column} < %s OR {column} > %s)
                            GROUP BY {column}
                            ORDER BY count DESC
                            LIMIT 5
                        """, (min_val, max_val))
                        
                        sample_data = [dict(row) for row in cursor.fetchall()]
                        
                        issues.append(DataQualityIssue(
                            source_name=table_name,
                            table_name=table_name,
                            issue_type='out_of_range_values',
                            severity='warning',
                            description=f"Column {column} has {out_of_range_count} values outside expected range ({min_val}-{max_val})",
                            affected_records=out_of_range_count,
                            detected_at=datetime.now(),
                            sample_data={'column': column, 'samples': sample_data}
                        ))
                
        except Exception as e:
            logger.error(f"Range validation failed for {table_name}: {e}")
            issues.append(DataQualityIssue(
                source_name=table_name,
                table_name=table_name,
                issue_type='range_check_failed',
                severity='warning',
                description=f"Range validation failed: {str(e)}",
                affected_records=0,
                detected_at=datetime.now()
            ))
        
        return issues
    
    def check_null_percentages(self, table_name: str) -> List[DataQualityIssue]:
        """Check for excessive null values"""
        issues = []
        
        if table_name not in self.quality_rules:
            return issues
        
        null_tolerance = self.quality_rules[table_name].get('null_tolerance', 0.05)
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get total record count
                cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
                total_records = cursor.fetchone()['total']
                
                if total_records == 0:
                    return issues
                
                # Get column null percentages
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND data_type IN ('integer', 'numeric', 'text', 'varchar', 'real', 'double precision')
                """, (table_name,))
                
                columns = [row['column_name'] for row in cursor.fetchall()]
                
                for column in columns:
                    cursor.execute(f"""
                        SELECT COUNT(*) as null_count
                        FROM {table_name}
                        WHERE {column} IS NULL
                    """)
                    
                    null_count = cursor.fetchone()['null_count']
                    null_percentage = null_count / total_records
                    
                    if null_percentage > null_tolerance:
                        issues.append(DataQualityIssue(
                            source_name=table_name,
                            table_name=table_name,
                            issue_type='excessive_nulls',
                            severity='warning',
                            description=f"Column {column} has {null_percentage:.2%} null values (threshold: {null_tolerance:.2%})",
                            affected_records=null_count,
                            detected_at=datetime.now(),
                            sample_data={'column': column, 'null_percentage': null_percentage}
                        ))
                
        except Exception as e:
            logger.error(f"Null check failed for {table_name}: {e}")
        
        return issues
    
    def validate_table_quality(self, table_name: str) -> List[DataQualityIssue]:
        """Run all quality validations for a table"""
        all_issues = []
        
        # Schema validation
        all_issues.extend(self.validate_table_schema(table_name))
        
        # Range validation
        all_issues.extend(self.validate_data_ranges(table_name))
        
        # Null percentage validation
        all_issues.extend(self.check_null_percentages(table_name))
        
        return all_issues
    
    def get_quality_report(self) -> Dict[str, Any]:
        """Get comprehensive data quality report"""
        monitored_tables = list(self.quality_rules.keys())
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'tables_checked': len(monitored_tables),
            'total_issues': 0,
            'critical_issues': 0,
            'warning_issues': 0,
            'table_reports': {}
        }
        
        for table_name in monitored_tables:
            table_issues = self.validate_table_quality(table_name)
            
            critical_count = len([i for i in table_issues if i.severity == 'critical'])
            warning_count = len([i for i in table_issues if i.severity == 'warning'])
            
            report['table_reports'][table_name] = {
                'total_issues': len(table_issues),
                'critical_issues': critical_count,
                'warning_issues': warning_count,
                'issues': [asdict(issue) for issue in table_issues]
            }
            
            report['total_issues'] += len(table_issues)
            report['critical_issues'] += critical_count
            report['warning_issues'] += warning_count
        
        return report

class CollectionFailureDetector:
    """Detects and reports data collection failures"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5439/mlb_data')
        
        # Expected collection patterns
        self.collection_patterns = {
            'fangraphs_batting': {
                'frequency_hours': 24,
                'min_records_per_collection': 100,
                'max_gap_hours': 48
            },
            'statcast_data': {
                'frequency_hours': 2,
                'min_records_per_collection': 10,
                'max_gap_hours': 6
            },
            'mlb_games': {
                'frequency_hours': 0.25,  # 15 minutes
                'min_records_per_collection': 1,
                'max_gap_hours': 1
            }
        }
        
        logger.info("Collection Failure Detector initialized")
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.database_url)
    
    def detect_collection_gaps(self, table_name: str, hours_back: int = 48) -> List[Dict[str, Any]]:
        """Detect gaps in data collection"""
        gaps = []
        
        if table_name not in self.collection_patterns:
            return gaps
        
        pattern = self.collection_patterns[table_name]
        expected_frequency = pattern['frequency_hours']
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get collection timestamps
                cursor.execute(f"""
                    SELECT 
                        DATE_TRUNC('hour', collected_at) as collection_hour,
                        COUNT(*) as records
                    FROM {table_name}
                    WHERE collected_at >= NOW() - INTERVAL '{hours_back} hours'
                    GROUP BY DATE_TRUNC('hour', collected_at)
                    ORDER BY collection_hour DESC
                """)
                
                collections = cursor.fetchall()
                
                if not collections:
                    gaps.append({
                        'type': 'no_data',
                        'severity': 'critical',
                        'description': f"No data found in {table_name} for last {hours_back} hours",
                        'start_time': None,
                        'end_time': None,
                        'duration_hours': hours_back
                    })
                    return gaps
                
                # Check for gaps between collections
                for i in range(len(collections) - 1):
                    current_time = collections[i]['collection_hour']
                    next_time = collections[i + 1]['collection_hour']
                    
                    gap_duration = (current_time - next_time).total_seconds() / 3600
                    
                    if gap_duration > expected_frequency * 2:  # More than 2x expected frequency
                        gaps.append({
                            'type': 'collection_gap',
                            'severity': 'warning' if gap_duration < pattern['max_gap_hours'] else 'critical',
                            'description': f"Data collection gap of {gap_duration:.1f} hours (expected: {expected_frequency}h)",
                            'start_time': next_time.isoformat(),
                            'end_time': current_time.isoformat(),
                            'duration_hours': gap_duration
                        })
                
        except Exception as e:
            logger.error(f"Gap detection failed for {table_name}: {e}")
            gaps.append({
                'type': 'detection_error',
                'severity': 'critical',
                'description': f"Failed to detect gaps: {str(e)}",
                'start_time': None,
                'end_time': None,
                'duration_hours': None
            })
        
        return gaps
    
    def detect_low_volume_collections(self, table_name: str, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Detect collections with unusually low record counts"""
        issues = []
        
        if table_name not in self.collection_patterns:
            return issues
        
        pattern = self.collection_patterns[table_name]
        min_records = pattern['min_records_per_collection']
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Get recent collection volumes
                cursor.execute(f"""
                    SELECT 
                        DATE_TRUNC('hour', collected_at) as collection_hour,
                        COUNT(*) as records
                    FROM {table_name}
                    WHERE collected_at >= NOW() - INTERVAL '{hours_back} hours'
                    GROUP BY DATE_TRUNC('hour', collected_at)
                    HAVING COUNT(*) < %s
                    ORDER BY collection_hour DESC
                """, (min_records,))
                
                low_volume_collections = cursor.fetchall()
                
                for collection in low_volume_collections:
                    issues.append({
                        'type': 'low_volume',
                        'severity': 'warning',
                        'description': f"Low volume collection: {collection['records']} records (expected: ≥{min_records})",
                        'collection_time': collection['collection_hour'].isoformat(),
                        'record_count': collection['records'],
                        'expected_minimum': min_records
                    })
                
        except Exception as e:
            logger.error(f"Low volume detection failed for {table_name}: {e}")
        
        return issues
    
    def get_failure_report(self) -> Dict[str, Any]:
        """Generate comprehensive failure detection report"""
        monitored_tables = list(self.collection_patterns.keys())
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'tables_monitored': len(monitored_tables),
            'total_issues': 0,
            'critical_issues': 0,
            'warning_issues': 0,
            'table_reports': {}
        }
        
        for table_name in monitored_tables:
            # Detect gaps
            gaps = self.detect_collection_gaps(table_name)
            
            # Detect low volume
            low_volumes = self.detect_low_volume_collections(table_name)
            
            all_issues = gaps + low_volumes
            
            critical_count = len([i for i in all_issues if i.get('severity') == 'critical'])
            warning_count = len([i for i in all_issues if i.get('severity') == 'warning'])
            
            report['table_reports'][table_name] = {
                'total_issues': len(all_issues),
                'critical_issues': critical_count,
                'warning_issues': warning_count,
                'gaps': gaps,
                'low_volume_collections': low_volumes
            }
            
            report['total_issues'] += len(all_issues)
            report['critical_issues'] += critical_count
            report['warning_issues'] += warning_count
        
        return report

# Convenience functions for quick monitoring
def get_system_health() -> Dict[str, Any]:
    """Get quick system health overview"""
    tracker = DataFreshnessTracker()
    return tracker.get_system_health_summary()

def get_data_quality_status() -> Dict[str, Any]:
    """Get data quality status"""
    validator = DataQualityValidator()
    return validator.get_quality_report()

def get_collection_failures() -> Dict[str, Any]:
    """Get collection failure report"""
    detector = CollectionFailureDetector()
    return detector.get_failure_report()

def get_comprehensive_monitoring_report() -> Dict[str, Any]:
    """Get complete monitoring report"""
    return {
        'timestamp': datetime.now().isoformat(),
        'system_health': get_system_health(),
        'data_quality': get_data_quality_status(),
        'collection_failures': get_collection_failures()
    }
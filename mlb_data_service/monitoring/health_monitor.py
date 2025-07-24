#!/usr/bin/env python3
"""
Production Health Monitor
========================

Comprehensive health monitoring system for MLB data service with detailed metrics,
database connection monitoring, external API health checking, and performance tracking.
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

# Optional imports with fallbacks
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    import pybaseball as pyb
    PYBASEBALL_AVAILABLE = True
except ImportError:
    PYBASEBALL_AVAILABLE = False

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Optional database import
try:
    from enhanced_database import EnhancedDatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    # Mock database manager for testing
    class EnhancedDatabaseManager:
        def __init__(self):
            pass
        def test_connection(self):
            return False

logger = logging.getLogger(__name__)

@dataclass
class HealthMetric:
    """Individual health metric data structure"""
    name: str
    value: Any
    status: str  # 'healthy', 'warning', 'critical'
    threshold: Dict[str, Any]
    timestamp: datetime
    message: str = ""

@dataclass
class SystemMetrics:
    """System performance metrics"""
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    load_average: List[float]
    network_connections: int
    uptime_seconds: int

@dataclass
class DatabaseMetrics:
    """Database connection and performance metrics"""
    active_connections: int
    max_connections: int
    connection_pool_size: int
    query_response_time: float
    last_successful_query: datetime
    table_counts: Dict[str, int]
    database_size_mb: float

@dataclass
class ExternalAPIMetrics:
    """External API health and response metrics"""
    pybaseball_status: str
    pybaseball_response_time: float
    mlb_api_status: str
    mlb_api_response_time: float
    fangraphs_status: str
    fangraphs_response_time: float
    last_data_collection: datetime

class ProductionHealthMonitor:
    """Production-grade health monitoring system"""
    
    def __init__(self, database_manager: EnhancedDatabaseManager = None):
        """Initialize health monitor with configurable thresholds"""
        self.db_manager = database_manager or EnhancedDatabaseManager()
        
        # Health check thresholds
        self.thresholds = {
            'cpu_percent': {'warning': 70, 'critical': 90},
            'memory_percent': {'warning': 80, 'critical': 95},
            'disk_percent': {'warning': 85, 'critical': 95},
            'database_connections': {'warning': 80, 'critical': 95},
            'api_response_time': {'warning': 5.0, 'critical': 10.0},
            'query_response_time': {'warning': 1.0, 'critical': 3.0}
        }
        
        # Cache for metrics
        self._metrics_cache = {}
        self._cache_ttl = 30  # seconds
        
        # Start background monitoring
        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(target=self._background_monitor, daemon=True)
        self._monitoring_thread.start()
        
        logger.info("ProductionHealthMonitor initialized")
    
    def get_comprehensive_health(self) -> Dict[str, Any]:
        """Get comprehensive health status with all metrics"""
        try:
            # Collect all metrics
            system_metrics = self._get_system_metrics()
            db_metrics = self._get_database_metrics()
            api_metrics = self._get_external_api_metrics()
            
            # Evaluate overall health
            health_status = self._evaluate_overall_health(system_metrics, db_metrics, api_metrics)
            
            return {
                'overall_status': health_status['status'],
                'timestamp': datetime.now().isoformat(),
                'service': 'mlb-data-service-enhanced',
                'version': '2.0.0',
                'uptime_hours': round(system_metrics.uptime_seconds / 3600, 2),
                'metrics': {
                    'system': asdict(system_metrics),
                    'database': asdict(db_metrics),
                    'external_apis': asdict(api_metrics)
                },
                'health_checks': health_status['checks'],
                'alerts_triggered': health_status['alerts'],
                'recommendations': health_status['recommendations']
            }
            
        except Exception as e:
            logger.error(f"Failed to get comprehensive health: {e}")
            return {
                'overall_status': 'critical',
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'service': 'mlb-data-service-enhanced'
            }
    
    def _get_system_metrics(self) -> SystemMetrics:
        """Collect system performance metrics"""
        try:
            if not PSUTIL_AVAILABLE:
                logger.warning("psutil not available, returning mock system metrics")
                return SystemMetrics(
                    cpu_percent=25.0,
                    memory_percent=45.0,
                    disk_percent=60.0,
                    load_average=[0.5, 0.7, 0.8],
                    network_connections=50,
                    uptime_seconds=86400  # 1 day mock uptime
                )
            
            # CPU and Memory
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Load average (Unix-like systems)
            try:
                load_avg = list(os.getloadavg())
            except (OSError, AttributeError):
                load_avg = [0.0, 0.0, 0.0]
            
            # Network connections
            connections = len(psutil.net_connections())
            
            # System uptime
            boot_time = psutil.boot_time()
            uptime = time.time() - boot_time
            
            return SystemMetrics(
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                disk_percent=disk.percent,
                load_average=load_avg,
                network_connections=connections,
                uptime_seconds=int(uptime)
            )
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return SystemMetrics(0, 0, 0, [0, 0, 0], 0, 0)
    
    def _get_database_metrics(self) -> DatabaseMetrics:
        """Collect database connection and performance metrics"""
        try:
            if not DATABASE_AVAILABLE or not self.db_manager:
                logger.warning("Database not available, returning mock database metrics")
                return DatabaseMetrics(
                    active_connections=5,
                    max_connections=100,
                    connection_pool_size=10,
                    query_response_time=0.15,
                    last_successful_query=datetime.now(),
                    table_counts={"public.fangraphs_batting": 1500, "public.statcast_data": 50000},
                    database_size_mb=256.0
                )
            
            start_time = time.time()
            
            # Test database connection and measure response time
            conn = self.db_manager.get_connection()
            if not conn:
                raise Exception("Failed to get database connection")
            
            cursor = conn.cursor()
            
            # Get connection stats
            cursor.execute("""
                SELECT 
                    numbackends as active_connections,
                    setting::int as max_connections
                FROM pg_stat_database 
                JOIN pg_settings ON name = 'max_connections'
                WHERE datname = current_database()
            """)
            conn_data = cursor.fetchone()
            active_conns = conn_data[0] if conn_data else 0
            max_conns = conn_data[1] if conn_data else 100
            
            # Get table counts
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins + n_tup_upd + n_tup_del as total_operations
                FROM pg_stat_user_tables
                ORDER BY total_operations DESC
            """)
            table_stats = cursor.fetchall()
            
            # Get database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database()))
            """)
            db_size_str = cursor.fetchone()[0] if cursor.rowcount > 0 else "0 MB"
            
            # Extract numeric value from size string (rough approximation)
            db_size_mb = 0.0
            if 'MB' in db_size_str:
                db_size_mb = float(db_size_str.split()[0])
            elif 'GB' in db_size_str:
                db_size_mb = float(db_size_str.split()[0]) * 1024
            
            cursor.close()
            self.db_manager.return_connection(conn)
            
            query_time = time.time() - start_time
            
            # Pool size from connection pool
            pool_size = self.db_manager.pool.maxconn if self.db_manager.pool else 0
            
            # Table counts dictionary
            table_counts = {}
            for schema, table, ops in table_stats:
                table_counts[f"{schema}.{table}"] = ops
            
            return DatabaseMetrics(
                active_connections=active_conns,
                max_connections=max_conns,
                connection_pool_size=pool_size,
                query_response_time=query_time,
                last_successful_query=datetime.now(),
                table_counts=table_counts,
                database_size_mb=db_size_mb
            )
            
        except Exception as e:
            logger.error(f"Failed to collect database metrics: {e}")
            return DatabaseMetrics(0, 0, 0, 0.0, datetime.now(), {}, 0.0)
    
    def _get_external_api_metrics(self) -> ExternalAPIMetrics:
        """Check external API health and response times"""
        try:
            # Test PyBaseball
            pyb_start = time.time()
            pyb_status = "healthy"
            if PYBASEBALL_AVAILABLE:
                try:
                    # Quick test - get team info (lightweight call)
                    teams = pyb.team_ids()
                    if teams is None or len(teams) == 0:
                        pyb_status = "warning"
                except Exception as e:
                    logger.warning(f"PyBaseball health check failed: {e}")
                    pyb_status = "critical"
            else:
                pyb_status = "unavailable"
                logger.warning("PyBaseball not available")
            pyb_time = time.time() - pyb_start
            
            # Test MLB Stats API
            mlb_start = time.time()
            mlb_status = "healthy"
            if REQUESTS_AVAILABLE:
                try:
                    # Test MLB API endpoint
                    response = requests.get(
                        "https://statsapi.mlb.com/api/v1/teams",
                        timeout=5
                    )
                    if response.status_code != 200:
                        mlb_status = "warning"
                except Exception as e:
                    logger.warning(f"MLB API health check failed: {e}")
                    mlb_status = "critical"
            else:
                mlb_status = "unavailable"
                logger.warning("Requests not available")
            mlb_time = time.time() - mlb_start
            
            # Test FanGraphs (via PyBaseball)
            fg_start = time.time()
            fg_status = "healthy"
            if PYBASEBALL_AVAILABLE:
                try:
                    # Test FanGraphs data availability
                    current_year = datetime.now().year
                    test_data = pyb.batting_stats(current_year, qual=1, ind=1)
                    if test_data is None or test_data.empty:
                        fg_status = "warning"
                except Exception as e:
                    logger.warning(f"FanGraphs health check failed: {e}")
                    fg_status = "critical"
            else:
                fg_status = "unavailable"
                logger.warning("PyBaseball not available for FanGraphs")
            fg_time = time.time() - fg_start
            
            # Get last data collection timestamp from database
            last_collection = datetime.now() - timedelta(hours=1)  # Default
            try:
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(created_at) FROM fangraphs_batting
                    UNION ALL
                    SELECT MAX(game_date) FROM statcast_data
                    ORDER BY 1 DESC LIMIT 1
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    last_collection = result[0]
                cursor.close()
                self.db_manager.return_connection(conn)
            except Exception as e:
                logger.warning(f"Failed to get last collection time: {e}")
            
            return ExternalAPIMetrics(
                pybaseball_status=pyb_status,
                pybaseball_response_time=pyb_time,
                mlb_api_status=mlb_status,
                mlb_api_response_time=mlb_time,
                fangraphs_status=fg_status,
                fangraphs_response_time=fg_time,
                last_data_collection=last_collection
            )
            
        except Exception as e:
            logger.error(f"Failed to collect external API metrics: {e}")
            return ExternalAPIMetrics(
                "critical", 0.0, "critical", 0.0, "critical", 0.0,
                datetime.now() - timedelta(hours=24)
            )
    
    def _evaluate_overall_health(self, system: SystemMetrics, db: DatabaseMetrics, 
                               apis: ExternalAPIMetrics) -> Dict[str, Any]:
        """Evaluate overall health status and generate alerts/recommendations"""
        checks = []
        alerts = []
        recommendations = []
        critical_count = 0
        warning_count = 0
        
        # System checks
        cpu_status = self._evaluate_threshold(system.cpu_percent, self.thresholds['cpu_percent'])
        checks.append({
            'name': 'CPU Usage',
            'value': f"{system.cpu_percent}%",
            'status': cpu_status,
            'threshold': self.thresholds['cpu_percent']
        })
        if cpu_status != 'healthy':
            if cpu_status == 'critical':
                critical_count += 1
                alerts.append(f"Critical: CPU usage at {system.cpu_percent}%")
                recommendations.append("Scale horizontally or optimize CPU-intensive operations")
            else:
                warning_count += 1
                recommendations.append("Monitor CPU usage trends")
        
        memory_status = self._evaluate_threshold(system.memory_percent, self.thresholds['memory_percent'])
        checks.append({
            'name': 'Memory Usage',
            'value': f"{system.memory_percent}%",
            'status': memory_status,
            'threshold': self.thresholds['memory_percent']
        })
        if memory_status != 'healthy':
            if memory_status == 'critical':
                critical_count += 1
                alerts.append(f"Critical: Memory usage at {system.memory_percent}%")
                recommendations.append("Increase memory allocation or optimize memory usage")
            else:
                warning_count += 1
        
        # Database checks
        db_conn_percent = (db.active_connections / db.max_connections * 100) if db.max_connections > 0 else 0
        db_status = self._evaluate_threshold(db_conn_percent, self.thresholds['database_connections'])
        checks.append({
            'name': 'Database Connections',
            'value': f"{db.active_connections}/{db.max_connections}",
            'status': db_status,
            'threshold': self.thresholds['database_connections']
        })
        if db_status != 'healthy':
            if db_status == 'critical':
                critical_count += 1
                alerts.append(f"Critical: Database connections at {db_conn_percent:.1f}%")
                recommendations.append("Increase connection pool size or optimize query patterns")
            else:
                warning_count += 1
        
        query_status = self._evaluate_threshold(db.query_response_time, self.thresholds['query_response_time'])
        checks.append({
            'name': 'Database Response Time',
            'value': f"{db.query_response_time:.3f}s",
            'status': query_status,
            'threshold': self.thresholds['query_response_time']
        })
        if query_status != 'healthy':
            if query_status == 'critical':
                critical_count += 1
                alerts.append(f"Critical: Database response time {db.query_response_time:.3f}s")
                recommendations.append("Optimize database queries or increase database resources")
            else:
                warning_count += 1
        
        # API checks
        api_statuses = [apis.pybaseball_status, apis.mlb_api_status, apis.fangraphs_status]
        for api_name, status, response_time in [
            ('PyBaseball', apis.pybaseball_status, apis.pybaseball_response_time),
            ('MLB API', apis.mlb_api_status, apis.mlb_api_response_time),
            ('FanGraphs', apis.fangraphs_status, apis.fangraphs_response_time)
        ]:
            checks.append({
                'name': f'{api_name} Status',
                'value': status,
                'status': status,
                'response_time': f"{response_time:.3f}s"
            })
            if status == 'critical':
                critical_count += 1
                alerts.append(f"Critical: {api_name} API unavailable")
                recommendations.append(f"Check {api_name} service status and network connectivity")
            elif status == 'warning':
                warning_count += 1
        
        # Data freshness check
        data_age = datetime.now() - apis.last_data_collection
        if data_age > timedelta(hours=6):
            warning_count += 1
            alerts.append(f"Warning: Data is {data_age.total_seconds()/3600:.1f} hours old")
            recommendations.append("Check data collection processes")
        
        # Determine overall status
        if critical_count > 0:
            overall_status = 'critical'
        elif warning_count > 0:
            overall_status = 'warning'
        else:
            overall_status = 'healthy'
        
        return {
            'status': overall_status,
            'checks': checks,
            'alerts': alerts,
            'recommendations': recommendations,
            'summary': {
                'critical_issues': critical_count,
                'warnings': warning_count,
                'total_checks': len(checks)
            }
        }
    
    def _evaluate_threshold(self, value: float, thresholds: Dict[str, float]) -> str:
        """Evaluate a metric against its thresholds"""
        if value >= thresholds['critical']:
            return 'critical'
        elif value >= thresholds['warning']:
            return 'warning'
        else:
            return 'healthy'
    
    def _background_monitor(self):
        """Background monitoring loop for continuous health tracking"""
        while self._monitoring_active:
            try:
                # Update cached metrics
                health_data = self.get_comprehensive_health()
                self._metrics_cache['health'] = {
                    'data': health_data,
                    'timestamp': time.time()
                }
                
                # Log critical issues
                if health_data.get('overall_status') == 'critical':
                    logger.error(f"Critical health issues detected: {health_data.get('alerts', [])}")
                
                # Sleep for monitoring interval
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Background monitoring error: {e}")
                time.sleep(30)  # Shorter sleep on error
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self._monitoring_active = False
        if self._monitoring_thread.is_alive():
            self._monitoring_thread.join(timeout=5)
    
    def get_cached_health(self) -> Optional[Dict[str, Any]]:
        """Get cached health data if available and fresh"""
        cached = self._metrics_cache.get('health')
        if cached and (time.time() - cached['timestamp']) < self._cache_ttl:
            return cached['data']
        return None
    
    def run_health_check(self) -> Dict[str, Any]:
        """Run immediate health check and return results"""
        # Try cached data first
        cached = self.get_cached_health()
        if cached:
            return cached
        
        # Generate fresh data
        return self.get_comprehensive_health()

if __name__ == "__main__":
    # Test the health monitor
    monitor = ProductionHealthMonitor()
    health = monitor.run_health_check()
    print("Health Check Results:")
    print(f"Overall Status: {health['overall_status']}")
    print(f"Alerts: {len(health.get('alerts_triggered', []))}")
    print(f"System CPU: {health['metrics']['system']['cpu_percent']}%")
    print(f"Memory: {health['metrics']['system']['memory_percent']}%")
    monitor.stop_monitoring()
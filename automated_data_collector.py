#!/usr/bin/env python3
"""
StatEdge Automated Data Collection System
========================================

Scheduled data collection for FanGraphs, Statcast, and other MLB data sources.
Integrates with Slack notifications and maintains collection history.
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pybaseball
import requests

# Import our existing components
try:
    from slack_notifier import StatEdgeSlackNotifier
except ImportError:
    StatEdgeSlackNotifier = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollectionJob:
    """Represents a single data collection job"""
    
    def __init__(self, job_id: str, job_type: str, config: Dict[str, Any]):
        self.job_id = job_id
        self.job_type = job_type  # 'fangraphs_batting', 'fangraphs_pitching', 'statcast'
        self.config = config
        self.created_at = datetime.now()
        self.last_run = None
        self.next_run = None
        self.status = 'pending'
        self.records_collected = 0
        self.error_message = None
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'config': self.config,
            'created_at': self.created_at.isoformat(),
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'status': self.status,
            'records_collected': self.records_collected,
            'error_message': self.error_message
        }

class StatEdgeDataCollector:
    """Main automated data collection system for StatEdge"""
    
    def __init__(self, slack_webhook_url: str = None):
        self.scheduler = BackgroundScheduler()
        self.jobs: Dict[str, DataCollectionJob] = {}
        self.collection_history: List[Dict[str, Any]] = []
        self.is_running = False
        
        # Initialize Slack notifier
        self.slack_notifier = None
        if StatEdgeSlackNotifier and slack_webhook_url:
            self.slack_notifier = StatEdgeSlackNotifier(slack_webhook_url)
        
        # Initialize collection history database
        self._init_history_db()
        
        # Set up default collection jobs
        self._setup_default_jobs()
        
        logger.info("✅ StatEdge Data Collector initialized")
    
    def _init_history_db(self):
        """Initialize SQLite database for collection history"""
        self.db_path = "collection_history.db"
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS collection_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT NOT NULL,
                    records_collected INTEGER DEFAULT 0,
                    error_message TEXT,
                    duration_seconds REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def _setup_default_jobs(self):
        """Set up default scheduled collection jobs"""
        
        # Daily FanGraphs batting collection at 2 AM
        fangraphs_batting_job = DataCollectionJob(
            job_id="fangraphs_batting_daily",
            job_type="fangraphs_batting",
            config={
                "schedule": "0 2 * * *",  # Daily at 2 AM
                "season": "current",
                "min_pa": 10,
                "description": "Daily FanGraphs batting data collection",
                "max_retries": 3,
                "retry_delay_seconds": 60
            }
        )
        
        # Daily FanGraphs pitching collection at 2:30 AM
        fangraphs_pitching_job = DataCollectionJob(
            job_id="fangraphs_pitching_daily", 
            job_type="fangraphs_pitching",
            config={
                "schedule": "30 2 * * *",  # Daily at 2:30 AM
                "season": "current",
                "min_ip": 5,
                "description": "Daily FanGraphs pitching data collection",
                "max_retries": 3,
                "retry_delay_seconds": 60
            }
        )
        
        # Daily Statcast collection at 3 AM
        statcast_job = DataCollectionJob(
            job_id="statcast_daily",
            job_type="statcast",
            config={
                "schedule": "0 3 * * *",  # Daily at 3 AM
                "days_back": 1,
                "description": "Daily Statcast data collection",
                "max_retries": 2,  # Statcast can be more flaky
                "retry_delay_seconds": 120
            }
        )
        
        self.jobs[fangraphs_batting_job.job_id] = fangraphs_batting_job
        self.jobs[fangraphs_pitching_job.job_id] = fangraphs_pitching_job
        self.jobs[statcast_job.job_id] = statcast_job
        
        logger.info(f"📋 Set up {len(self.jobs)} default collection jobs")
    
    def start_scheduler(self):
        """Start the automated data collection scheduler"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        try:
            # Start scheduler first
            self.scheduler.start()
            
            # Add jobs to scheduler
            for job in self.jobs.values():
                self.scheduler.add_job(
                    func=self._execute_collection_job,
                    trigger=CronTrigger.from_crontab(job.config['schedule']),
                    args=[job.job_id],
                    id=job.job_id,
                    name=job.config['description'],
                    replace_existing=True
                )
                
                # Calculate next run time after scheduler is started
                scheduler_job = self.scheduler.get_job(job.job_id)
                if scheduler_job:
                    job.next_run = scheduler_job.next_run_time
            
            self.is_running = True
            
            logger.info("🚀 StatEdge data collection scheduler started")
            
            # Send Slack notification
            if self.slack_notifier and self.slack_notifier.enabled:
                self.slack_notifier.send_notification(
                    "🤖 *Automated Data Collection Started*\n\nStatEdge scheduler is now running with 3 daily collection jobs.",
                    "success",
                    metadata={
                        "Jobs Scheduled": len(self.jobs),
                        "Next Collection": min([job.next_run for job in self.jobs.values()]).strftime("%Y-%m-%d %H:%M:%S"),
                        "Status": "Running"
                    }
                )
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            if self.slack_notifier and self.slack_notifier.enabled:
                self.slack_notifier.critical_system_alert(f"Data collection scheduler failed to start: {str(e)}")
            raise
    
    def stop_scheduler(self):
        """Stop the automated data collection scheduler"""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return
        
        try:
            self.scheduler.shutdown(wait=False)
            self.is_running = False
            logger.info("🛑 StatEdge data collection scheduler stopped")
            
            # Send Slack notification
            if self.slack_notifier and self.slack_notifier.enabled:
                self.slack_notifier.send_notification(
                    "⏹️ *Automated Data Collection Stopped*\n\nStatEdge scheduler has been stopped.",
                    "warning",
                    metadata={"Status": "Stopped"}
                )
                
        except Exception as e:
            logger.error(f"Failed to stop scheduler: {e}")
    
    def _execute_collection_job(self, job_id: str):
        """Execute a specific data collection job with retry logic"""
        job = self.jobs.get(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        max_retries = job.config.get('max_retries', 3)
        retry_delay = job.config.get('retry_delay_seconds', 60)
        
        logger.info(f"🔄 Starting collection job: {job_id} (max retries: {max_retries})")
        start_time = datetime.now()
        
        # Update job status
        job.status = 'running'
        job.last_run = start_time
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"🔄 Retry attempt {attempt}/{max_retries} for job {job_id}")
                    # Wait before retry (exponential backoff)
                    wait_time = retry_delay * (2 ** (attempt - 1))
                    time.sleep(min(wait_time, 300))  # Max 5 minutes wait
                
                # Execute the specific collection based on job type
                if job.job_type == 'fangraphs_batting':
                    records = self._collect_fangraphs_batting(job.config)
                elif job.job_type == 'fangraphs_pitching':
                    records = self._collect_fangraphs_pitching(job.config)
                elif job.job_type == 'statcast':
                    records = self._collect_statcast(job.config)
                else:
                    raise ValueError(f"Unknown job type: {job.job_type}")
                
                # Success
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                job.status = 'completed'
                job.records_collected = records
                job.error_message = None
                
                # Log to history database
                self._log_collection_run(
                    job.job_type, start_time, end_time, 'success', 
                    records, None, duration, attempt
                )
                
                success_msg = f"✅ Collection job {job_id} completed: {records} records in {duration:.1f}s"
                if attempt > 0:
                    success_msg += f" (succeeded on attempt {attempt + 1})"
                
                logger.info(success_msg)
                
                # Send success notification to Slack
                if self.slack_notifier and self.slack_notifier.enabled:
                    metadata = {
                        "Records Collected": f"{records:,}",
                        "Duration": f"{duration:.1f} seconds",
                        "Data Source": job.job_type.replace('_', ' ').title()
                    }
                    
                    if attempt > 0:
                        metadata["Retry Attempt"] = f"{attempt + 1}/{max_retries + 1}"
                    
                    self.slack_notifier.send_notification(
                        f"📊 *Data Collection Complete*\n\n{job.config['description']}: {records:,} records collected",
                        "success",
                        metadata=metadata
                    )
                
                # Success - break out of retry loop
                break
                
            except Exception as e:
                last_error = e
                logger.warning(f"⚠️ Collection job {job_id} attempt {attempt + 1} failed: {e}")
                
                # If this is the last attempt, log as failure
                if attempt == max_retries:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    job.status = 'failed'
                    job.error_message = str(e)
                    
                    # Log to history database
                    self._log_collection_run(
                        job.job_type, start_time, end_time, 'failed', 
                        0, str(e), duration, max_retries
                    )
                    
                    logger.error(f"❌ Collection job {job_id} failed after {max_retries + 1} attempts: {e}")
                    
                    # Send failure notification to Slack
                    if self.slack_notifier and self.slack_notifier.enabled:
                        self.slack_notifier.send_notification(
                            f"❌ *Data Collection Failed*\n\n{job.config['description']} failed after {max_retries + 1} attempts",
                            "error",
                            metadata={
                                "Error": str(e)[:200] + "..." if len(str(e)) > 200 else str(e),
                                "Total Attempts": max_retries + 1,
                                "Duration": f"{duration:.1f} seconds",
                                "Data Source": job.job_type.replace('_', ' ').title()
                            }
                        )
        
        # Update next run time
        scheduler_job = self.scheduler.get_job(job_id)
        if scheduler_job:
            job.next_run = scheduler_job.next_run_time
    
    def _collect_fangraphs_batting(self, config: Dict[str, Any]) -> int:
        """Collect FanGraphs batting data and save to database"""
        season = config.get('season', datetime.now().year)
        if season == 'current':
            season = datetime.now().year
        
        min_pa = config.get('min_pa', 10)
        
        logger.info(f"🏏 Collecting FanGraphs batting data for {season} (min PA: {min_pa})")
        
        try:
            # Use pybaseball to collect data
            batting_data = pybaseball.batting_stats(season, qual=min_pa)
            
            if batting_data is not None and len(batting_data) > 0:
                # Try to save to PostgreSQL database
                try:
                    import psycopg2
                    from sqlalchemy import create_engine
                    import pandas as pd
                    
                    # Database connection string
                    db_url = os.getenv('DATABASE_URL', 'postgresql://statedge_user:statedge_secure_2024@localhost:5439/mlb_data')
                    
                    engine = create_engine(db_url)
                    
                    # Save to database with conflict handling
                    batting_data.to_sql(
                        f'fangraphs_batting_{season}', 
                        engine, 
                        if_exists='replace',  # Replace table each time for fresh data
                        index=False,
                        method='multi'
                    )
                    
                    records_count = len(batting_data)
                    logger.info(f"📊 Saved {records_count} FanGraphs batting records to database")
                    return records_count
                    
                except Exception as db_error:
                    logger.warning(f"Database save failed, using in-memory count: {db_error}")
                    # Fallback to just counting records
                    records_count = len(batting_data)
                    logger.info(f"📊 Collected {records_count} batting records (not saved to DB)")
                    return records_count
                    
            else:
                logger.warning("No batting data collected")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs batting data: {e}")
            raise
    
    def _collect_fangraphs_pitching(self, config: Dict[str, Any]) -> int:
        """Collect FanGraphs pitching data and save to database"""
        season = config.get('season', datetime.now().year)
        if season == 'current':
            season = datetime.now().year
        
        min_ip = config.get('min_ip', 5)
        
        logger.info(f"⚾ Collecting FanGraphs pitching data for {season} (min IP: {min_ip})")
        
        try:
            # Use pybaseball to collect data
            pitching_data = pybaseball.pitching_stats(season, qual=min_ip)
            
            if pitching_data is not None and len(pitching_data) > 0:
                # Try to save to PostgreSQL database
                try:
                    import psycopg2
                    from sqlalchemy import create_engine
                    
                    # Database connection string
                    db_url = os.getenv('DATABASE_URL', 'postgresql://statedge_user:statedge_secure_2024@localhost:5439/mlb_data')
                    
                    engine = create_engine(db_url)
                    
                    # Save to database
                    pitching_data.to_sql(
                        f'fangraphs_pitching_{season}', 
                        engine, 
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )
                    
                    records_count = len(pitching_data)
                    logger.info(f"📊 Saved {records_count} FanGraphs pitching records to database")
                    return records_count
                    
                except Exception as db_error:
                    logger.warning(f"Database save failed, using in-memory count: {db_error}")
                    records_count = len(pitching_data)
                    logger.info(f"📊 Collected {records_count} pitching records (not saved to DB)")
                    return records_count
                    
            else:
                logger.warning("No pitching data collected")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs pitching data: {e}")
            raise
    
    def _collect_statcast(self, config: Dict[str, Any]) -> int:
        """Collect Statcast data and save to database"""
        days_back = config.get('days_back', 1)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"📡 Collecting Statcast data from {start_date} to {end_date}")
        
        try:
            # Use pybaseball to collect Statcast data
            statcast_data = pybaseball.statcast(
                start_dt=start_date.strftime('%Y-%m-%d'),
                end_dt=end_date.strftime('%Y-%m-%d')
            )
            
            if statcast_data is not None and len(statcast_data) > 0:
                # Try to save to PostgreSQL database
                try:
                    import psycopg2
                    from sqlalchemy import create_engine
                    
                    # Database connection string
                    db_url = os.getenv('DATABASE_URL', 'postgresql://statedge_user:statedge_secure_2024@localhost:5439/mlb_data')
                    
                    engine = create_engine(db_url)
                    
                    # Create table name with date range
                    table_name = f'statcast_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}'
                    
                    # Save to database
                    statcast_data.to_sql(
                        table_name, 
                        engine, 
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )
                    
                    records_count = len(statcast_data)
                    logger.info(f"📊 Saved {records_count} Statcast records to database")
                    return records_count
                    
                except Exception as db_error:
                    logger.warning(f"Database save failed, using in-memory count: {db_error}")
                    records_count = len(statcast_data)
                    logger.info(f"📊 Collected {records_count} Statcast records (not saved to DB)")
                    return records_count
                    
            else:
                logger.warning("No Statcast data collected")
                return 0
                
        except Exception as e:
            logger.error(f"Statcast collection failed: {e}")
            raise
    
    def _log_collection_run(self, job_type: str, start_time: datetime, end_time: datetime, 
                           status: str, records: int, error: str, duration: float, retry_attempt: int = 0):
        """Log collection run to history database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # First, check if retry_attempt column exists, if not add it
                cursor = conn.execute("PRAGMA table_info(collection_runs)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'retry_attempt' not in columns:
                    conn.execute("ALTER TABLE collection_runs ADD COLUMN retry_attempt INTEGER DEFAULT 0")
                
                conn.execute("""
                    INSERT INTO collection_runs 
                    (job_type, start_time, end_time, status, records_collected, error_message, duration_seconds, retry_attempt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (job_type, start_time, end_time, status, records, error, duration, retry_attempt))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to log collection run: {e}")
    
    def get_collection_status(self) -> Dict[str, Any]:
        """Get current status of all collection jobs"""
        status = {
            'scheduler_running': self.is_running,
            'total_jobs': len(self.jobs),
            'jobs': [job.to_dict() for job in self.jobs.values()],
            'next_collection': None,
            'last_collection': None
        }
        
        # Find next and last collection times
        next_runs = [job.next_run for job in self.jobs.values() if job.next_run]
        if next_runs:
            status['next_collection'] = min(next_runs).isoformat()
        
        last_runs = [job.last_run for job in self.jobs.values() if job.last_run]
        if last_runs:
            status['last_collection'] = max(last_runs).isoformat()
        
        return status
    
    def get_collection_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent collection history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT * FROM collection_runs 
                    ORDER BY start_time DESC 
                    LIMIT ?
                """, (limit,))
                
                columns = [desc[0] for desc in cursor.description]
                history = []
                
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    history.append(record)
                
                return history
                
        except Exception as e:
            logger.error(f"Failed to get collection history: {e}")
            return []

    def trigger_manual_collection(self, job_type: str) -> Dict[str, Any]:
        """Manually trigger a collection job"""
        matching_jobs = [job for job in self.jobs.values() if job.job_type == job_type]
        
        if not matching_jobs:
            return {
                'status': 'error',
                'message': f'No job found for type: {job_type}'
            }
        
        job = matching_jobs[0]  # Use first matching job
        
        logger.info(f"🔧 Manually triggering collection job: {job.job_id}")
        
        # Execute in background thread to avoid blocking
        thread = threading.Thread(
            target=self._execute_collection_job,
            args=[job.job_id],
            daemon=True
        )
        thread.start()
        
        return {
            'status': 'success',
            'message': f'Manual collection started for {job_type}',
            'job_id': job.job_id
        }
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get comprehensive collection statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Overall stats
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                        SUM(records_collected) as total_records,
                        AVG(duration_seconds) as avg_duration
                    FROM collection_runs
                """)
                overall_stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
                
                # Stats by job type
                cursor = conn.execute("""
                    SELECT 
                        job_type,
                        COUNT(*) as runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                        SUM(records_collected) as total_records,
                        AVG(duration_seconds) as avg_duration,
                        MAX(start_time) as last_run
                    FROM collection_runs
                    GROUP BY job_type
                """)
                
                job_type_stats = {}
                for row in cursor.fetchall():
                    job_type = row[0]
                    job_type_stats[job_type] = {
                        'runs': row[1],
                        'successful': row[2],
                        'success_rate': (row[2] / row[1] * 100) if row[1] > 0 else 0,
                        'total_records': row[3] or 0,
                        'avg_duration': round(row[4] or 0, 2),
                        'last_run': row[5]
                    }
                
                # Recent failure analysis
                cursor = conn.execute("""
                    SELECT job_type, error_message, start_time
                    FROM collection_runs
                    WHERE status = 'failed' 
                    ORDER BY start_time DESC
                    LIMIT 5
                """)
                
                recent_failures = []
                for row in cursor.fetchall():
                    recent_failures.append({
                        'job_type': row[0],
                        'error': row[1],
                        'timestamp': row[2]
                    })
                
                return {
                    'overall': overall_stats,
                    'by_job_type': job_type_stats,
                    'recent_failures': recent_failures,
                    'scheduler_running': self.is_running,
                    'active_jobs': len(self.jobs)
                }
                
        except Exception as e:
            logger.error(f"Failed to get collection stats: {e}")
            return {
                'error': str(e),
                'scheduler_running': self.is_running,
                'active_jobs': len(self.jobs)
            }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check of the data collection system"""
        health_status = {
            'overall_status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'checks': {},
            'recommendations': []
        }
        
        # Check 1: Scheduler status
        if self.is_running:
            health_status['checks']['scheduler'] = {'status': 'healthy', 'message': 'Scheduler is running'}
        else:
            health_status['checks']['scheduler'] = {'status': 'unhealthy', 'message': 'Scheduler is not running'}
            health_status['overall_status'] = 'unhealthy'
            health_status['recommendations'].append('Start the data collector scheduler')
        
        # Check 2: Job configuration
        if len(self.jobs) > 0:
            health_status['checks']['jobs'] = {
                'status': 'healthy', 
                'message': f'{len(self.jobs)} collection jobs configured'
            }
        else:
            health_status['checks']['jobs'] = {'status': 'warning', 'message': 'No collection jobs configured'}
            health_status['recommendations'].append('Configure data collection jobs')
        
        # Check 3: Recent collection success rate
        try:
            stats = self.get_collection_stats()
            if 'overall' in stats and stats['overall']['total_runs'] > 0:
                success_rate = (stats['overall']['successful_runs'] / stats['overall']['total_runs']) * 100
                
                if success_rate >= 90:
                    health_status['checks']['success_rate'] = {
                        'status': 'healthy',
                        'message': f'Collection success rate: {success_rate:.1f}%'
                    }
                elif success_rate >= 70:
                    health_status['checks']['success_rate'] = {
                        'status': 'warning',
                        'message': f'Collection success rate: {success_rate:.1f}%'
                    }
                    health_status['recommendations'].append('Investigate collection failures')
                else:
                    health_status['checks']['success_rate'] = {
                        'status': 'unhealthy',
                        'message': f'Low collection success rate: {success_rate:.1f}%'
                    }
                    health_status['overall_status'] = 'unhealthy'
                    health_status['recommendations'].append('Critical: Fix collection failures immediately')
            else:
                health_status['checks']['success_rate'] = {
                    'status': 'warning',
                    'message': 'No collection history available'
                }
        except Exception as e:
            health_status['checks']['success_rate'] = {
                'status': 'error',
                'message': f'Unable to check success rate: {str(e)}'
            }
        
        # Check 4: Database connectivity
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("SELECT 1").fetchone()
            health_status['checks']['database'] = {'status': 'healthy', 'message': 'Database accessible'}
        except Exception as e:
            health_status['checks']['database'] = {
                'status': 'unhealthy',
                'message': f'Database error: {str(e)}'
            }
            health_status['overall_status'] = 'unhealthy'
            health_status['recommendations'].append('Fix database connectivity issues')
        
        # Check 5: Slack integration
        if self.slack_notifier and self.slack_notifier.enabled:
            health_status['checks']['slack'] = {'status': 'healthy', 'message': 'Slack notifications enabled'}
        else:
            health_status['checks']['slack'] = {'status': 'warning', 'message': 'Slack notifications disabled'}
            health_status['recommendations'].append('Configure Slack webhook for notifications')
        
        return health_status


# Global collector instance
_collector_instance = None

def get_data_collector(slack_webhook_url: str = None) -> StatEdgeDataCollector:
    """Get or create the global data collector instance"""
    global _collector_instance
    
    if _collector_instance is None:
        webhook_url = slack_webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        _collector_instance = StatEdgeDataCollector(webhook_url)
    
    return _collector_instance


if __name__ == "__main__":
    # Test the data collector
    collector = get_data_collector()
    
    print("🏢 StatEdge Automated Data Collection Test")
    print("=" * 50)
    
    try:
        collector.start_scheduler()
        
        print("✅ Scheduler started successfully")
        print(f"📋 Jobs configured: {len(collector.jobs)}")
        
        status = collector.get_collection_status()
        print(f"⏰ Next collection: {status['next_collection']}")
        
        # Keep running for a few seconds to test
        time.sleep(5)
        
        print("\n🧪 Testing manual collection...")
        result = collector.trigger_manual_collection('fangraphs_batting')
        print(f"Manual trigger result: {result}")
        
        # Wait a bit for the manual collection to start
        time.sleep(10)
        
        print("\n📊 Collection status:")
        status = collector.get_collection_status()
        for job_data in status['jobs']:
            print(f"  {job_data['job_type']}: {job_data['status']} (records: {job_data['records_collected']})")
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping scheduler...")
        collector.stop_scheduler()
        print("✅ Test completed")
    
    except Exception as e:
        print(f"❌ Test failed: {e}")
        if _collector_instance:
            _collector_instance.stop_scheduler()
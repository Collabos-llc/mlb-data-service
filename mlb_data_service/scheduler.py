#!/usr/bin/env python3
"""
MLB Data Service - Automated Scheduler
=====================================

Handles automated daily data collection at 7 AM using APScheduler.
"""

import logging
import os
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import requests
import json
import asyncio
from .external_apis import get_api_manager
from .enhanced_database import EnhancedDatabaseManager

logger = logging.getLogger(__name__)

class MLBDataScheduler:
    """Manages automated real-time data collection with multiple refresh intervals"""
    
    def __init__(self, service_url: str = "http://localhost:8001"):
        self.service_url = service_url
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
        # Initialize API manager and database
        self.api_manager = get_api_manager()
        self.db_manager = EnhancedDatabaseManager()
        
        self._setup_jobs()
        
    def _setup_jobs(self):
        """Setup scheduled jobs for real-time data collection with multiple refresh intervals"""
        
        # FanGraphs data collection - Daily at 7:00 AM
        self.scheduler.add_job(
            func=self._fangraphs_collection_job,
            trigger=CronTrigger(hour=7, minute=0),  # 7:00 AM every day
            id='daily_fangraphs_collection',
            name='Daily FanGraphs Data Collection',
            replace_existing=True,
            misfire_grace_time=300  # Allow 5 minutes grace period
        )
        
        # Statcast data collection - Every hour during baseball season
        self.scheduler.add_job(
            func=self._statcast_collection_job,
            trigger=IntervalTrigger(hours=1),  # Every hour
            id='hourly_statcast_collection',
            name='Hourly Statcast Data Collection',
            replace_existing=True,
            misfire_grace_time=300
        )
        
        # MLB games data collection - Every 15 minutes during game days
        self.scheduler.add_job(
            func=self._games_collection_job,
            trigger=IntervalTrigger(minutes=15),  # Every 15 minutes
            id='live_games_collection',
            name='Live MLB Games Collection',
            replace_existing=True,
            misfire_grace_time=120
        )
        
        # Health check job every hour
        self.scheduler.add_job(
            func=self._health_check_job,
            trigger=CronTrigger(minute=0),  # Every hour at minute 0
            id='hourly_health_check',
            name='Hourly Health Check',
            replace_existing=True
        )
        
        # Data freshness check every 30 minutes
        self.scheduler.add_job(
            func=self._data_freshness_check_job,
            trigger=IntervalTrigger(minutes=30),  # Every 30 minutes
            id='data_freshness_check',
            name='Data Freshness Check',
            replace_existing=True
        )
        
        # Weekly cleanup job - Sundays at 2 AM
        self.scheduler.add_job(
            func=self._weekly_cleanup_job,
            trigger=CronTrigger(day_of_week=6, hour=2, minute=0),  # Sunday at 2 AM
            id='weekly_cleanup',
            name='Weekly Data Cleanup',
            replace_existing=True
        )
        
        logger.info("Real-time scheduled jobs configured:")
        logger.info("  - FanGraphs collection: Daily at 7:00 AM")
        logger.info("  - Statcast collection: Every hour")
        logger.info("  - Games collection: Every 15 minutes")
        logger.info("  - Health checks: Every hour")
        logger.info("  - Data freshness checks: Every 30 minutes")
        logger.info("  - Weekly cleanup: Sunday 2:00 AM")
    
    def _daily_collection_job(self):
        """Execute daily data collection sequence"""
        logger.info("Starting automated daily MLB data collection...")
        
        collection_results = {
            'started_at': datetime.now().isoformat(),
            'completed_jobs': [],
            'failed_jobs': [],
            'total_records': 0
        }
        
        # Collection sequence: Players -> Games -> Statcast
        jobs = [
            {
                'name': 'players',
                'endpoint': '/api/v1/collect/players',
                'payload': {'limit': 100}
            },
            {
                'name': 'games',
                'endpoint': '/api/v1/collect/games',
                'payload': {}
            },
            {
                'name': 'statcast',
                'endpoint': '/api/v1/collect/statcast',
                'payload': {'days_back': 1, 'limit': 200}
            }
        ]
        
        for job in jobs:
            try:
                logger.info(f"Collecting {job['name']} data...")
                
                response = requests.post(
                    f"{self.service_url}{job['endpoint']}",
                    json=job['payload'],
                    timeout=300  # 5 minute timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    records_key = {
                        'players': 'players_collected',
                        'games': 'games_collected', 
                        'statcast': 'records_collected'
                    }.get(job['name'], 'records_collected')
                    
                    records_collected = result.get(records_key, 0)
                    collection_results['completed_jobs'].append({
                        'job': job['name'],
                        'records': records_collected,
                        'status': 'success'
                    })
                    collection_results['total_records'] += records_collected
                    
                    logger.info(f"✅ {job['name']} collection completed: {records_collected} records")
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    collection_results['failed_jobs'].append({
                        'job': job['name'],
                        'error': error_msg
                    })
                    logger.error(f"❌ {job['name']} collection failed: {error_msg}")
                    
            except Exception as e:
                error_msg = str(e)
                collection_results['failed_jobs'].append({
                    'job': job['name'],
                    'error': error_msg
                })
                logger.error(f"❌ {job['name']} collection failed: {error_msg}")
        
        collection_results['completed_at'] = datetime.now().isoformat()
        
        # Log final results
        success_count = len(collection_results['completed_jobs'])
        total_jobs = len(jobs)
        
        if success_count == total_jobs:
            logger.info(f"🎉 Daily collection completed successfully!")
            logger.info(f"   Total records collected: {collection_results['total_records']}")
        else:
            logger.warning(f"⚠️ Daily collection completed with issues:")
            logger.warning(f"   Successful: {success_count}/{total_jobs}")
            logger.warning(f"   Failed jobs: {[job['job'] for job in collection_results['failed_jobs']]}")
        
        return collection_results
    
    def _fangraphs_collection_job(self):
        """Execute FanGraphs data collection job"""
        logger.info("Starting automated FanGraphs data collection...")
        
        try:
            result = self.api_manager.collect_live_fangraphs_data(force_refresh=True)
            
            if result.get('status') == 'error':
                logger.error(f"FanGraphs collection failed: {result.get('error')}")
                return result
            
            batting_records = result.get('batting_records', 0)
            pitching_records = result.get('pitching_records', 0)
            total_records = batting_records + pitching_records
            
            logger.info(f"✅ FanGraphs collection completed: {total_records} records")
            logger.info(f"   - Batting: {batting_records} records")
            logger.info(f"   - Pitching: {pitching_records} records")
            
            return result
            
        except Exception as e:
            logger.error(f"FanGraphs collection job failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _statcast_collection_job(self):
        """Execute Statcast data collection job"""
        logger.info("Starting automated Statcast data collection...")
        
        try:
            result = self.api_manager.collect_live_statcast_data(hours_back=6)
            
            if result.get('status') == 'error':
                logger.error(f"Statcast collection failed: {result.get('error')}")
                return result
            
            if result.get('status') == 'skipped':
                logger.info("Statcast collection skipped - data is fresh")
                return result
            
            records_collected = result.get('records_collected', 0)
            logger.info(f"✅ Statcast collection completed: {records_collected} records")
            
            return result
            
        except Exception as e:
            logger.error(f"Statcast collection job failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _games_collection_job(self):
        """Execute MLB games data collection job"""
        logger.info("Starting automated MLB games data collection...")
        
        try:
            result = self.api_manager.collect_live_games_data(days_ahead=3)
            
            if result.get('status') == 'error':
                logger.error(f"Games collection failed: {result.get('error')}")
                return result
            
            if result.get('status') == 'skipped':
                logger.debug("Games collection skipped - data is fresh")
                return result
            
            games_collected = result.get('games_collected', 0)
            logger.info(f"✅ Games collection completed: {games_collected} games")
            
            return result
            
        except Exception as e:
            logger.error(f"Games collection job failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _data_freshness_check_job(self):
        """Check data freshness across all sources"""
        logger.debug("Checking data freshness...")
        
        try:
            status = self.api_manager.get_data_freshness_status()
            
            # Log any stale data warnings
            for source, info in status.items():
                if source == 'current_time':
                    continue
                    
                if isinstance(info, dict) and info.get('needs_refresh'):
                    if source == 'fangraphs' and info.get('hours_since_update', 0) > 48:
                        logger.warning(f"⚠️ FanGraphs data is stale: {info.get('hours_since_update')} hours old")
                    elif source == 'statcast' and info.get('hours_since_update', 0) > 6:
                        logger.warning(f"⚠️ Statcast data is stale: {info.get('hours_since_update')} hours old")
                    elif source == 'games' and info.get('minutes_since_update', 0) > 60:
                        logger.warning(f"⚠️ Games data is stale: {info.get('minutes_since_update')} minutes old")
            
            return status
            
        except Exception as e:
            logger.error(f"Data freshness check failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _health_check_job(self):
        """Perform hourly health check"""
        try:
            response = requests.get(f"{self.service_url}/health", timeout=30)
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get('status') == 'healthy':
                    logger.debug("✅ Hourly health check passed")
                else:
                    logger.warning(f"⚠️ Health check warning: {health_data}")
            else:
                logger.error(f"❌ Health check failed: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"❌ Health check error: {e}")
    
    def _weekly_cleanup_job(self):
        """Perform weekly data cleanup"""
        logger.info("Starting weekly data cleanup...")
        
        # This would typically clean old data, optimize database, etc.
        # For now, just log the action
        logger.info("✅ Weekly cleanup completed (placeholder)")
    
    def _job_listener(self, event):
        """Listen to job execution events"""
        if event.exception:
            logger.error(f"Job {event.job_id} crashed: {event.exception}")
        else:
            logger.debug(f"Job {event.job_id} executed successfully")
    
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("MLB Data Scheduler started")
            
            # Log next run times
            jobs = self.scheduler.get_jobs()
            for job in jobs:
                logger.info(f"Next {job.name}: {job.next_run_time}")
        else:
            logger.warning("Scheduler is already running")
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("MLB Data Scheduler stopped")
    
    def trigger_daily_collection(self):
        """Manually trigger daily collection (for testing)"""
        logger.info("Manually triggering daily collection...")
        return self._daily_collection_job()
    
    def trigger_live_collection(self):
        """Manually trigger all live data collection"""
        logger.info("Manually triggering all live data collection...")
        
        results = {
            'started_at': datetime.now().isoformat(),
            'fangraphs': None,
            'statcast': None,
            'games': None
        }
        
        # Execute all collection jobs
        try:
            results['fangraphs'] = self._fangraphs_collection_job()
        except Exception as e:
            results['fangraphs'] = {'status': 'error', 'error': str(e)}
        
        try:
            results['statcast'] = self._statcast_collection_job()
        except Exception as e:
            results['statcast'] = {'status': 'error', 'error': str(e)}
        
        try:
            results['games'] = self._games_collection_job()
        except Exception as e:
            results['games'] = {'status': 'error', 'error': str(e)}
        
        results['completed_at'] = datetime.now().isoformat()
        
        # Log summary
        successful_jobs = sum(1 for job_result in [results['fangraphs'], results['statcast'], results['games']] 
                             if job_result and job_result.get('status') != 'error')
        
        logger.info(f"Live data collection completed: {successful_jobs}/3 jobs successful")
        
        return results
    
    def get_job_status(self):
        """Get status of scheduled jobs"""
        if not self.scheduler.running:
            return {'status': 'stopped', 'jobs': []}
        
        jobs = self.scheduler.get_jobs()
        job_info = []
        
        for job in jobs:
            job_info.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return {
            'status': 'running',
            'jobs': job_info,
            'scheduler_info': {
                'timezone': str(self.scheduler.timezone),
                'state': 'running' if self.scheduler.running else 'stopped'
            },
            'data_freshness': self.api_manager.get_data_freshness_status() if hasattr(self, 'api_manager') else {}
        }

# Global scheduler instance
_scheduler_instance = None

def get_scheduler(service_url: str = "http://localhost:8001") -> MLBDataScheduler:
    """Get or create scheduler instance"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = MLBDataScheduler(service_url)
    return _scheduler_instance

def start_scheduler(service_url: str = "http://localhost:8001"):
    """Start the global scheduler"""
    scheduler = get_scheduler(service_url)
    scheduler.start()
    return scheduler

def stop_scheduler():
    """Stop the global scheduler"""
    global _scheduler_instance
    if _scheduler_instance:
        _scheduler_instance.stop()
        _scheduler_instance = None
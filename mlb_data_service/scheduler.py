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
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import requests
import json

logger = logging.getLogger(__name__)

class MLBDataScheduler:
    """Manages automated daily data collection"""
    
    def __init__(self, service_url: str = "http://localhost:8001"):
        self.service_url = service_url
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self._setup_jobs()
        
    def _setup_jobs(self):
        """Setup scheduled jobs for daily data collection"""
        
        # Daily data collection at 7:00 AM
        self.scheduler.add_job(
            func=self._daily_collection_job,
            trigger=CronTrigger(hour=7, minute=0),  # 7:00 AM every day
            id='daily_mlb_collection',
            name='Daily MLB Data Collection',
            replace_existing=True,
            misfire_grace_time=300  # Allow 5 minutes grace period
        )
        
        # Health check job every hour
        self.scheduler.add_job(
            func=self._health_check_job,
            trigger=CronTrigger(minute=0),  # Every hour at minute 0
            id='hourly_health_check',
            name='Hourly Health Check',
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
        
        logger.info("Scheduled jobs configured:")
        logger.info("  - Daily collection: 7:00 AM")
        logger.info("  - Health checks: Every hour")
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
            }
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
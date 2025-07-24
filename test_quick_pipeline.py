#!/usr/bin/env python3
"""
Quick Real-Time Data Pipeline Test
==================================

Quick validation test for the real-time data ingestion pipeline.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'mlb_data_service'))

import logging
from mlb_data_service.external_apis import ExternalAPIManager
from mlb_data_service.enhanced_database import EnhancedDatabaseManager
from mlb_data_service.scheduler import MLBDataScheduler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quick_test():
    """Quick test of the real-time pipeline components"""
    
    logger.info("🚀 Starting quick real-time pipeline validation...")
    
    # Test 1: Database connection
    try:
        db_manager = EnhancedDatabaseManager()
        if db_manager.test_connection():
            logger.info("✅ Database connection: PASSED")
        else:
            logger.error("❌ Database connection: FAILED")
            return False
    except Exception as e:
        logger.error(f"❌ Database connection error: {e}")
        return False
    
    # Test 2: API manager initialization
    try:
        api_manager = ExternalAPIManager(db_manager)
        status = api_manager.get_data_freshness_status()
        if 'current_time' in status:
            logger.info("✅ API manager initialization: PASSED")
        else:
            logger.error("❌ API manager initialization: FAILED")
            return False
    except Exception as e:
        logger.error(f"❌ API manager initialization error: {e}")
        return False
    
    # Test 3: Scheduler setup
    try:
        scheduler = MLBDataScheduler()
        job_status = scheduler.get_job_status()
        
        # Check if scheduler is configured (even if not running)
        if job_status.get('status') == 'stopped':
            # For stopped scheduler, check if jobs are configured by looking at the scheduler directly
            all_jobs = scheduler.scheduler.get_jobs()
            if len(all_jobs) >= 5:
                logger.info(f"✅ Scheduler setup: PASSED ({len(all_jobs)} jobs configured, not running)")
            else:
                logger.error(f"❌ Scheduler setup: FAILED (only {len(all_jobs)} jobs)")
                return False
        else:
            jobs = job_status.get('jobs', [])
            if len(jobs) >= 5:
                logger.info(f"✅ Scheduler setup: PASSED ({len(jobs)} jobs running)")
            else:
                logger.error(f"❌ Scheduler setup: FAILED (only {len(jobs)} jobs)")
                return False
    except Exception as e:
        logger.error(f"❌ Scheduler setup error: {e}")
        return False
    
    # Test 4: Database statistics
    try:
        stats = db_manager.get_database_stats()
        logger.info("✅ Database statistics: PASSED")
        logger.info(f"   - FanGraphs batting: {stats.get('fangraphs_batting_count', 0)} records")
        logger.info(f"   - FanGraphs pitching: {stats.get('fangraphs_pitching_count', 0)} records")
        logger.info(f"   - Statcast: {stats.get('statcast_count', 0)} records")
    except Exception as e:
        logger.error(f"❌ Database statistics error: {e}")
        return False
    
    # Test 5: Data freshness monitoring
    try:
        freshness = api_manager.get_data_freshness_status()
        logger.info("✅ Data freshness monitoring: PASSED")
        for source in ['fangraphs', 'statcast', 'games']:
            if source in freshness:
                needs_refresh = freshness[source].get('needs_refresh', True)
                logger.info(f"   - {source.capitalize()}: needs_refresh={needs_refresh}")
    except Exception as e:
        logger.error(f"❌ Data freshness monitoring error: {e}")
        return False
    
    # Cleanup
    try:
        api_manager.close()
        db_manager.close()
    except:
        pass
    
    logger.info("🎉 All quick tests PASSED! Real-time pipeline is operational.")
    return True

if __name__ == "__main__":
    success = quick_test()
    if success:
        print("\n✅ Quick pipeline validation completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Quick pipeline validation failed!")
        sys.exit(1)
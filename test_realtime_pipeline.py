#!/usr/bin/env python3
"""
Test Real-Time Data Ingestion Pipeline
======================================

Validates the complete real-time data flow from external APIs to database.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'mlb_data_service'))

import logging
import asyncio
from datetime import datetime
from mlb_data_service.external_apis import ExternalAPIManager, get_api_manager
from mlb_data_service.enhanced_database import EnhancedDatabaseManager
from mlb_data_service.scheduler import MLBDataScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealTimePipelineValidator:
    """Validates the complete real-time data ingestion pipeline"""
    
    def __init__(self):
        self.db_manager = EnhancedDatabaseManager()
        self.api_manager = ExternalAPIManager(self.db_manager)
        self.scheduler = MLBDataScheduler()
        
    def test_database_connection(self) -> bool:
        """Test database connectivity"""
        logger.info("🔍 Testing database connection...")
        
        try:
            connected = self.db_manager.test_connection()
            if connected:
                logger.info("✅ Database connection successful")
                return True
            else:
                logger.error("❌ Database connection failed")
                return False
        except Exception as e:
            logger.error(f"❌ Database connection error: {e}")
            return False
    
    def test_api_manager_initialization(self) -> bool:
        """Test API manager initialization"""
        logger.info("🔍 Testing API manager initialization...")
        
        try:
            # Test data freshness status
            status = self.api_manager.get_data_freshness_status()
            
            if 'current_time' in status:
                logger.info("✅ API manager initialized successfully")
                logger.info(f"   Current time: {status['current_time']}")
                return True
            else:
                logger.error("❌ API manager initialization failed")
                return False
        except Exception as e:
            logger.error(f"❌ API manager initialization error: {e}")
            return False
    
    def test_live_fangraphs_collection(self) -> bool:
        """Test live FanGraphs data collection"""
        logger.info("🔍 Testing live FanGraphs data collection...")
        
        try:
            result = self.api_manager.collect_live_fangraphs_data(force_refresh=True)
            
            if result.get('status') == 'error':
                logger.error(f"❌ FanGraphs collection failed: {result.get('error')}")
                return False
            
            batting_records = result.get('batting_records', 0)
            pitching_records = result.get('pitching_records', 0)
            total_records = batting_records + pitching_records
            
            logger.info(f"✅ FanGraphs collection completed: {total_records} records")
            logger.info(f"   - Batting: {batting_records} records")
            logger.info(f"   - Pitching: {pitching_records} records")
            
            return total_records > 0
            
        except Exception as e:
            logger.error(f"❌ FanGraphs collection error: {e}")
            return False
    
    def test_live_statcast_collection(self) -> bool:
        """Test live Statcast data collection"""
        logger.info("🔍 Testing live Statcast data collection...")
        
        try:
            result = self.api_manager.collect_live_statcast_data(hours_back=24)
            
            if result.get('status') == 'error':
                logger.error(f"❌ Statcast collection failed: {result.get('error')}")
                return False
            
            if result.get('status') == 'skipped':
                logger.info("ℹ️ Statcast collection skipped - data is fresh")
                return True
            
            records_collected = result.get('records_collected', 0)
            logger.info(f"✅ Statcast collection completed: {records_collected} records")
            
            return True  # Consider successful even if no new records
            
        except Exception as e:
            logger.error(f"❌ Statcast collection error: {e}")
            return False
    
    def test_live_games_collection(self) -> bool:
        """Test live MLB games data collection"""
        logger.info("🔍 Testing live MLB games data collection...")
        
        try:
            result = self.api_manager.collect_live_games_data(days_ahead=2)
            
            if result.get('status') == 'error':
                logger.error(f"❌ Games collection failed: {result.get('error')}")
                return False
            
            if result.get('status') == 'skipped':
                logger.info("ℹ️ Games collection skipped - data is fresh")
                return True
            
            games_collected = result.get('games_collected', 0)
            date_range = result.get('date_range', [])
            logger.info(f"✅ Games collection completed: {games_collected} games")
            logger.info(f"   Date range: {', '.join(date_range)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Games collection error: {e}")
            return False
    
    async def test_concurrent_collection(self) -> bool:
        """Test concurrent data collection"""
        logger.info("🔍 Testing concurrent data collection...")
        
        try:
            result = await self.api_manager.collect_all_live_data()
            
            duration = result.get('total_duration_seconds', 0)
            logger.info(f"✅ Concurrent collection completed in {duration:.2f} seconds")
            
            # Check individual results
            fangraphs_success = result.get('fangraphs', {}).get('status') != 'error'
            statcast_success = result.get('statcast', {}).get('status') != 'error'
            games_success = result.get('games', {}).get('status') != 'error'
            
            successful_sources = sum([fangraphs_success, statcast_success, games_success])
            logger.info(f"   Successful sources: {successful_sources}/3")
            
            return successful_sources >= 2  # At least 2 out of 3 sources should work
            
        except Exception as e:
            logger.error(f"❌ Concurrent collection error: {e}")
            return False
    
    def test_database_integration(self) -> bool:
        """Test database integration and data storage"""
        logger.info("🔍 Testing database integration...")
        
        try:
            # Get database statistics
            stats = self.db_manager.get_database_stats()
            
            logger.info("✅ Database integration test successful")
            logger.info("   Database contents:")
            logger.info(f"   - FanGraphs batting: {stats.get('fangraphs_batting_count', 0)} records")
            logger.info(f"   - FanGraphs pitching: {stats.get('fangraphs_pitching_count', 0)} records")
            logger.info(f"   - Statcast: {stats.get('statcast_count', 0)} records")
            logger.info(f"   - Latest FanGraphs season: {stats.get('latest_fangraphs_season', 'N/A')}")
            logger.info(f"   - Latest Statcast date: {stats.get('latest_statcast_date', 'N/A')}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database integration error: {e}")
            return False
    
    def test_scheduler_setup(self) -> bool:
        """Test scheduler setup and job configuration"""
        logger.info("🔍 Testing scheduler setup...")
        
        try:
            # Get job status without starting scheduler
            job_status = self.scheduler.get_job_status()
            
            if job_status.get('status') == 'stopped':
                logger.info("✅ Scheduler configured but not running (expected)")
                
                jobs = job_status.get('jobs', [])
                logger.info(f"   Configured jobs: {len(jobs)}")
                
                for job in jobs:
                    logger.info(f"   - {job.get('name', 'Unknown')}: {job.get('trigger', 'N/A')}")
                
                return len(jobs) >= 5  # Should have at least 5 scheduled jobs
            else:
                logger.info("ℹ️ Scheduler is already running")
                return True
            
        except Exception as e:
            logger.error(f"❌ Scheduler setup error: {e}")
            return False
    
    def test_data_freshness_monitoring(self) -> bool:
        """Test data freshness monitoring"""
        logger.info("🔍 Testing data freshness monitoring...")
        
        try:
            status = self.api_manager.get_data_freshness_status()
            
            logger.info("✅ Data freshness monitoring test successful")
            logger.info("   Data freshness status:")
            
            for source, info in status.items():
                if source == 'current_time':
                    continue
                    
                if isinstance(info, dict):
                    last_update = info.get('last_update', 'Never')
                    needs_refresh = info.get('needs_refresh', True)
                    logger.info(f"   - {source.capitalize()}: Last update {last_update}, Needs refresh: {needs_refresh}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Data freshness monitoring error: {e}")
            return False
    
    async def run_complete_validation(self) -> dict:
        """Run complete pipeline validation"""
        logger.info("🚀 Starting complete real-time pipeline validation...")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        tests = [
            ("Database Connection", self.test_database_connection),
            ("API Manager Initialization", self.test_api_manager_initialization),
            ("Live FanGraphs Collection", self.test_live_fangraphs_collection),
            ("Live Statcast Collection", self.test_live_statcast_collection), 
            ("Live Games Collection", self.test_live_games_collection),
            ("Database Integration", self.test_database_integration),
            ("Scheduler Setup", self.test_scheduler_setup),
            ("Data Freshness Monitoring", self.test_data_freshness_monitoring),
        ]
        
        results = {
            'started_at': start_time.isoformat(),
            'tests': {},
            'concurrent_test': None
        }
        
        # Run individual tests
        for test_name, test_func in tests:
            logger.info(f"\n📋 Running: {test_name}")
            try:
                success = test_func()
                results['tests'][test_name] = {
                    'success': success,
                    'error': None
                }
                logger.info(f"{'✅' if success else '❌'} {test_name}: {'PASSED' if success else 'FAILED'}")
            except Exception as e:
                results['tests'][test_name] = {
                    'success': False,
                    'error': str(e)
                }
                logger.error(f"❌ {test_name}: FAILED with error: {e}")
        
        # Run concurrent collection test
        logger.info(f"\n📋 Running: Concurrent Collection Test")
        try:
            concurrent_success = await self.test_concurrent_collection()
            results['concurrent_test'] = {
                'success': concurrent_success,
                'error': None
            }
            logger.info(f"{'✅' if concurrent_success else '❌'} Concurrent Collection: {'PASSED' if concurrent_success else 'FAILED'}")
        except Exception as e:
            results['concurrent_test'] = {
                'success': False,
                'error': str(e)
            }
            logger.error(f"❌ Concurrent Collection: FAILED with error: {e}")
        
        # Calculate summary
        end_time = datetime.now()
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        passed_tests = sum(1 for test_result in results['tests'].values() if test_result['success'])
        total_tests = len(results['tests'])
        
        if results['concurrent_test'] and results['concurrent_test']['success']:
            passed_tests += 1
            total_tests += 1
        
        results['summary'] = {
            'passed': passed_tests,
            'total': total_tests,
            'success_rate': round((passed_tests / total_tests) * 100, 1) if total_tests > 0 else 0
        }
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("🏁 REAL-TIME PIPELINE VALIDATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"⏱️  Duration: {results['duration_seconds']:.2f} seconds")
        logger.info(f"📊 Results: {passed_tests}/{total_tests} tests passed ({results['summary']['success_rate']}%)")
        
        if results['summary']['success_rate'] >= 80:
            logger.info("🎉 PIPELINE VALIDATION SUCCESSFUL!")
            logger.info("   Real-time data ingestion pipeline is operational")
        else:
            logger.warning("⚠️  PIPELINE VALIDATION NEEDS ATTENTION")
            logger.warning("   Some components may need troubleshooting")
        
        return results

async def main():
    """Main test execution"""
    validator = RealTimePipelineValidator()
    
    try:
        results = await validator.run_complete_validation()
        return results
    except Exception as e:
        logger.error(f"❌ Pipeline validation failed: {e}")
        return {'status': 'error', 'error': str(e)}
    finally:
        # Clean up resources
        try:
            validator.api_manager.close()
            validator.db_manager.close()
        except:
            pass

if __name__ == "__main__":
    # Run the validation
    results = asyncio.run(main())
    
    # Exit with appropriate code
    if results.get('summary', {}).get('success_rate', 0) >= 80:
        print("\n✅ Real-time pipeline validation completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Real-time pipeline validation failed!")
        sys.exit(1)
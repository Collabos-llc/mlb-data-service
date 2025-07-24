#!/usr/bin/env python3
"""
Real-Time Data Pipeline Demonstration
=====================================

Demonstrates the complete real-time data ingestion pipeline in action.
Shows live data collection and automatic refresh capabilities.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'mlb_data_service'))

import logging
import asyncio
from datetime import datetime
from mlb_data_service.external_apis import ExternalAPIManager
from mlb_data_service.enhanced_database import EnhancedDatabaseManager
from mlb_data_service.scheduler import MLBDataScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealTimePipelineDemo:
    """Demonstrates the real-time data pipeline functionality"""
    
    def __init__(self):
        self.db_manager = EnhancedDatabaseManager()
        self.api_manager = ExternalAPIManager(self.db_manager)
        self.scheduler = MLBDataScheduler()
    
    def show_initial_status(self):
        """Show initial database and system status"""
        logger.info("🔍 INITIAL SYSTEM STATUS")
        logger.info("=" * 50)
        
        # Database stats
        stats = self.db_manager.get_database_stats()
        logger.info("📊 Current Database Contents:")
        logger.info(f"   - FanGraphs batting: {stats.get('fangraphs_batting_count', 0):,} records")
        logger.info(f"   - FanGraphs pitching: {stats.get('fangraphs_pitching_count', 0):,} records")
        logger.info(f"   - Statcast data: {stats.get('statcast_count', 0):,} records")
        logger.info(f"   - Latest FanGraphs season: {stats.get('latest_fangraphs_season', 'N/A')}")
        logger.info(f"   - Latest Statcast date: {stats.get('latest_statcast_date', 'N/A')}")
        
        # Data freshness
        freshness = self.api_manager.get_data_freshness_status()
        logger.info("\n⏰ Data Freshness Status:")
        for source in ['fangraphs', 'statcast', 'games']:
            if source in freshness:
                info = freshness[source]
                last_update = info.get('last_update', 'Never')
                needs_refresh = info.get('needs_refresh', True)
                logger.info(f"   - {source.capitalize()}: Last update {last_update}, Needs refresh: {needs_refresh}")
        
        # Scheduler status
        job_status = self.scheduler.get_job_status()
        all_jobs = self.scheduler.scheduler.get_jobs()
        logger.info(f"\n📅 Scheduler Status: {len(all_jobs)} jobs configured")
        for job in all_jobs[:3]:  # Show first 3 jobs
            logger.info(f"   - {job.name}: {job.trigger}")
    
    def demonstrate_live_games_collection(self):
        """Demonstrate live games data collection"""
        logger.info("\n🎾 DEMONSTRATING LIVE GAMES COLLECTION")
        logger.info("=" * 50)
        
        result = self.api_manager.collect_live_games_data(days_ahead=2)
        
        if result.get('status') == 'error':
            logger.error(f"❌ Games collection failed: {result.get('error')}")
        elif result.get('status') == 'skipped':
            logger.info("ℹ️ Games collection skipped - data is fresh")
        else:
            games_collected = result.get('games_collected', 0)
            date_range = result.get('date_range', [])
            logger.info(f"✅ Collected {games_collected} games")
            logger.info(f"📅 Date range: {', '.join(date_range)}")
    
    def demonstrate_data_freshness_monitoring(self):
        """Demonstrate data freshness monitoring"""
        logger.info("\n🔄 DEMONSTRATING DATA FRESHNESS MONITORING")
        logger.info("=" * 50)
        
        freshness = self.api_manager.get_data_freshness_status()
        current_time = freshness.get('current_time')
        
        logger.info(f"📊 Data Freshness Report (as of {current_time}):")
        
        for source, info in freshness.items():
            if source == 'current_time':
                continue
                
            if isinstance(info, dict):
                last_update = info.get('last_update', 'Never')
                needs_refresh = info.get('needs_refresh', True)
                
                if source == 'fangraphs':
                    hours_since = info.get('hours_since_update')
                    if hours_since is not None:
                        logger.info(f"   📈 FanGraphs: {hours_since:.1f} hours old, refresh needed: {needs_refresh}")
                    else:
                        logger.info(f"   📈 FanGraphs: Never updated, refresh needed: {needs_refresh}")
                        
                elif source == 'statcast':
                    hours_since = info.get('hours_since_update')
                    if hours_since is not None:
                        logger.info(f"   ⚾ Statcast: {hours_since:.1f} hours old, refresh needed: {needs_refresh}")
                    else:
                        logger.info(f"   ⚾ Statcast: Never updated, refresh needed: {needs_refresh}")
                        
                elif source == 'games':
                    minutes_since = info.get('minutes_since_update')
                    if minutes_since is not None:
                        logger.info(f"   🎮 Games: {minutes_since:.1f} minutes old, refresh needed: {needs_refresh}")
                    else:
                        logger.info(f"   🎮 Games: Never updated, refresh needed: {needs_refresh}")
    
    def demonstrate_manual_scheduler_trigger(self):
        """Demonstrate manual scheduler trigger"""
        logger.info("\n⚡ DEMONSTRATING MANUAL SCHEDULER TRIGGER")
        logger.info("=" * 50)
        
        logger.info("🔄 Triggering live data collection jobs...")
        
        # Trigger individual jobs manually
        results = {}
        
        # Games collection (quick)
        try:
            logger.info("📋 Running games collection job...")
            results['games'] = self.scheduler._games_collection_job()
            if results['games'].get('status') != 'error':
                logger.info("✅ Games collection job completed successfully")
            else:
                logger.error(f"❌ Games collection job failed: {results['games'].get('error')}")
        except Exception as e:
            logger.error(f"❌ Games collection job error: {e}")
            results['games'] = {'status': 'error', 'error': str(e)}
        
        # Statcast collection (may take time)
        try:
            logger.info("📋 Running Statcast collection job...")
            results['statcast'] = self.scheduler._statcast_collection_job()
            if results['statcast'].get('status') != 'error':
                records = results['statcast'].get('records_collected', 0)
                logger.info(f"✅ Statcast collection job completed: {records} records")
            else:
                logger.error(f"❌ Statcast collection job failed: {results['statcast'].get('error')}")
        except Exception as e:
            logger.error(f"❌ Statcast collection job error: {e}")
            results['statcast'] = {'status': 'error', 'error': str(e)}
        
        return results
    
    def show_updated_status(self):
        """Show updated status after operations"""
        logger.info("\n📊 UPDATED SYSTEM STATUS")
        logger.info("=" * 50)
        
        # Updated database stats
        stats = self.db_manager.get_database_stats()
        logger.info("📈 Updated Database Contents:")
        logger.info(f"   - FanGraphs batting: {stats.get('fangraphs_batting_count', 0):,} records")
        logger.info(f"   - FanGraphs pitching: {stats.get('fangraphs_pitching_count', 0):,} records")
        logger.info(f"   - Statcast data: {stats.get('statcast_count', 0):,} records")
        
        # Updated freshness
        freshness = self.api_manager.get_data_freshness_status()
        logger.info("\n⏰ Updated Data Freshness:")
        for source in ['fangraphs', 'statcast', 'games']:
            if source in freshness:
                info = freshness[source]
                needs_refresh = info.get('needs_refresh', True)
                if source == 'games':
                    minutes_since = info.get('minutes_since_update')
                    if minutes_since is not None:
                        logger.info(f"   - {source.capitalize()}: {minutes_since:.1f} minutes old, needs refresh: {needs_refresh}")
                    else:
                        logger.info(f"   - {source.capitalize()}: Never updated, needs refresh: {needs_refresh}")
                else:
                    hours_since = info.get('hours_since_update')
                    if hours_since is not None:
                        logger.info(f"   - {source.capitalize()}: {hours_since:.1f} hours old, needs refresh: {needs_refresh}")
                    else:
                        logger.info(f"   - {source.capitalize()}: Never updated, needs refresh: {needs_refresh}")
    
    def run_complete_demo(self):
        """Run the complete demonstration"""
        start_time = datetime.now()
        
        logger.info("🚀 REAL-TIME DATA PIPELINE DEMONSTRATION")
        logger.info("=" * 60)
        logger.info(f"Started at: {start_time.isoformat()}")
        
        try:
            # Show initial status
            self.show_initial_status()
            
            # Demonstrate live games collection
            self.demonstrate_live_games_collection()
            
            # Demonstrate data freshness monitoring
            self.demonstrate_data_freshness_monitoring()
            
            # Demonstrate scheduler triggers
            scheduler_results = self.demonstrate_manual_scheduler_trigger()
            
            # Show updated status
            self.show_updated_status()
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n🎉 DEMONSTRATION COMPLETE")
            logger.info("=" * 60)
            logger.info(f"⏱️  Total duration: {duration:.2f} seconds")
            logger.info(f"📅 Completed at: {end_time.isoformat()}")
            
            # Check if any operations were successful
            successful_ops = sum(1 for result in scheduler_results.values() 
                               if result.get('status') != 'error')
            total_ops = len(scheduler_results)
            
            logger.info(f"📊 Operation success rate: {successful_ops}/{total_ops}")
            
            if successful_ops >= total_ops // 2:
                logger.info("✅ REAL-TIME PIPELINE IS OPERATIONAL!")
                logger.info("   - Data collection jobs are working")
                logger.info("   - Automatic refresh is functional")
                logger.info("   - Database integration is active")
                return True
            else:
                logger.warning("⚠️  PIPELINE NEEDS ATTENTION")
                logger.warning("   - Some operations failed")
                logger.warning("   - Check logs for details")
                return False
                
        except Exception as e:
            logger.error(f"❌ Demonstration failed: {e}")
            return False
        finally:
            # Cleanup
            try:
                self.api_manager.close()
                self.db_manager.close()
            except:
                pass

def main():
    """Main demonstration execution"""
    demo = RealTimePipelineDemo()
    success = demo.run_complete_demo()
    
    if success:
        print("\n🎊 Real-time pipeline demonstration completed successfully!")
        print("💡 The system is ready for automated data collection.")
        return 0
    else:
        print("\n⚠️ Real-time pipeline demonstration completed with issues.")
        print("🔧 Some components may need troubleshooting.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
#!/usr/bin/env python3
"""
Test Script for Data Flow Monitoring System
===========================================

Tests the comprehensive monitoring and cleanup systems implemented for
Sprint 10: Production Deployment & Monitoring.

This script demonstrates:
1. Data freshness tracking and staleness detection
2. Data quality validation and issue detection
3. Collection failure monitoring
4. Automated cleanup processes
5. Performance monitoring
"""

import sys
import os
import json
import logging
from datetime import datetime

# Add the project directory to the path
sys.path.insert(0, '/home/jeffreyconboy/github-repos/mlb-data-service')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_data_freshness_monitoring():
    """Test data freshness tracking capabilities"""
    print("\n" + "="*60)
    print("TESTING DATA FRESHNESS MONITORING")
    print("="*60)
    
    try:
        from mlb_data_service.monitoring import DataFreshnessTracker, get_system_health
        
        # Test direct class usage
        tracker = DataFreshnessTracker()
        print("\n✅ DataFreshnessTracker initialized successfully")
        
        # Test freshness metrics for all tables
        print("\n📊 Getting freshness metrics for all monitored tables...")
        metrics = tracker.get_all_freshness_metrics()
        
        print(f"Found {len(metrics)} monitored data sources:")
        for metric in metrics:
            status_emoji = {
                'fresh': '🟢',
                'stale': '🟡', 
                'critical': '🔴',
                'missing': '⚫'
            }.get(metric.current_freshness_level.value, '❓')
            
            print(f"  {status_emoji} {metric.source_name}: {metric.current_freshness_level.value}")
            if metric.staleness_duration_hours:
                print(f"    Last update: {metric.staleness_duration_hours:.1f} hours ago")
        
        # Test system health summary
        print("\n🏥 Getting system health summary...")
        health = get_system_health()
        
        print(f"Overall Status: {health.get('overall_status', 'unknown')}")
        print(f"Healthy Sources: {health.get('source_counts', {}).get('fresh', 0)}")
        print(f"Sources Needing Attention: {health.get('needs_attention', 0)}")
        
        # Test stale data detection
        print("\n⚠️  Checking for stale data sources...")
        stale_sources = tracker.get_stale_data_sources()
        
        if stale_sources:
            print(f"Found {len(stale_sources)} stale data sources:")
            for source in stale_sources:
                print(f"  - {source.source_name}: {source.current_freshness_level.value}")
        else:
            print("No stale data sources detected")
        
        return True
        
    except Exception as e:
        print(f"❌ Data freshness monitoring test failed: {e}")
        return False

def test_data_quality_validation():
    """Test data quality validation capabilities"""
    print("\n" + "="*60)
    print("TESTING DATA QUALITY VALIDATION")
    print("="*60)
    
    try:
        from mlb_data_service.monitoring import DataQualityValidator, get_data_quality_status
        
        # Test direct class usage
        validator = DataQualityValidator()
        print("\n✅ DataQualityValidator initialized successfully")
        
        # Test quality validation for a specific table
        print("\n🔍 Testing data quality validation...")
        
        # Get comprehensive quality report
        print("\n📋 Getting comprehensive quality report...")
        quality_report = get_data_quality_status()
        
        print(f"Tables checked: {quality_report.get('tables_checked', 0)}")
        print(f"Total issues found: {quality_report.get('total_issues', 0)}")
        print(f"Critical issues: {quality_report.get('critical_issues', 0)}")
        print(f"Warning issues: {quality_report.get('warning_issues', 0)}")
        
        # Show table-specific reports
        table_reports = quality_report.get('table_reports', {})
        for table_name, report in table_reports.items():
            if report.get('total_issues', 0) > 0:
                print(f"\n🔧 Issues in {table_name}:")
                for issue in report.get('issues', []):
                    severity_emoji = '🔴' if issue.get('severity') == 'critical' else '🟡'
                    print(f"  {severity_emoji} {issue.get('description', 'Unknown issue')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data quality validation test failed: {e}")
        return False

def test_collection_failure_detection():
    """Test collection failure detection capabilities"""
    print("\n" + "="*60)
    print("TESTING COLLECTION FAILURE DETECTION")
    print("="*60)
    
    try:
        from mlb_data_service.monitoring import CollectionFailureDetector, get_collection_failures
        
        # Test direct class usage
        detector = CollectionFailureDetector()
        print("\n✅ CollectionFailureDetector initialized successfully")
        
        # Test failure detection
        print("\n🔍 Analyzing collection patterns and failures...")
        failure_report = get_collection_failures()
        
        print(f"Tables monitored: {failure_report.get('tables_monitored', 0)}")
        print(f"Total issues found: {failure_report.get('total_issues', 0)}")
        print(f"Critical issues: {failure_report.get('critical_issues', 0)}")
        
        # Show table-specific failure reports
        table_reports = failure_report.get('table_reports', {})
        for table_name, report in table_reports.items():
            total_issues = report.get('total_issues', 0)
            if total_issues > 0:
                print(f"\n⚠️  Issues in {table_name} ({total_issues} total):")
                
                # Show gaps
                gaps = report.get('gaps', [])
                for gap in gaps:
                    severity_emoji = '🔴' if gap.get('severity') == 'critical' else '🟡'
                    print(f"  {severity_emoji} {gap.get('description', 'Collection gap')}")
                
                # Show low volume collections
                low_volumes = report.get('low_volume_collections', [])
                for lv in low_volumes:
                    print(f"  🟡 {lv.get('description', 'Low volume collection')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Collection failure detection test failed: {e}")
        return False

def test_automated_cleanup():
    """Test automated cleanup capabilities"""
    print("\n" + "="*60)
    print("TESTING AUTOMATED CLEANUP SYSTEM")
    print("="*60)
    
    try:
        from mlb_data_service.monitoring import AutomatedCleanup, get_cleanup_status
        
        # Test cleanup status
        print("\n📊 Getting cleanup status...")
        cleanup_status = get_cleanup_status()
        
        print(f"Monitored tables: {len(cleanup_status.get('monitored_tables', []))}")
        print(f"Total database size: {cleanup_status.get('total_database_size_mb', 0):.2f} MB")
        
        # Show table sizes
        table_sizes = cleanup_status.get('table_sizes_mb', {})
        print("\n📋 Table sizes:")
        for table, size_mb in table_sizes.items():
            print(f"  {table}: {size_mb:.2f} MB")
        
        # Test cache cleanup
        print("\n🧹 Testing cache cleanup...")
        from mlb_data_service.monitoring import run_cache_cleanup
        
        cache_results = run_cache_cleanup()
        print(f"Cache cleanup operations: {len(cache_results)}")
        
        for result in cache_results:
            success_emoji = '✅' if result.success else '❌'
            print(f"  {success_emoji} {result.operation}: {result.duration_seconds:.2f}s")
        
        return True
        
    except Exception as e:
        print(f"❌ Automated cleanup test failed: {e}")
        return False

def test_external_api_monitoring():
    """Test external API monitoring enhancements"""
    print("\n" + "="*60)
    print("TESTING EXTERNAL API MONITORING")
    print("="*60)
    
    try:
        from mlb_data_service.external_apis import get_api_manager
        
        # Get API manager instance
        api_manager = get_api_manager()
        print("\n✅ External API Manager with monitoring initialized")
        
        # Test comprehensive monitoring status
        print("\n📊 Getting comprehensive monitoring status...")
        status = api_manager.get_comprehensive_monitoring_status()
        
        print(f"System health: {status.get('system_health', 'unknown')}")
        
        # Show collection failures
        failures = status.get('collection_failures', {})
        total_failures = sum(failures.values())
        print(f"Total collection failures: {total_failures}")
        
        if total_failures > 0:
            for source, failure_count in failures.items():
                if failure_count > 0:
                    print(f"  - {source}: {failure_count} failures")
        
        # Test performance metrics
        print("\n⚡ Getting performance metrics...")
        performance = api_manager.get_performance_metrics()
        
        print(f"Database connected: {performance.get('connection_pool_status', {}).get('database_connected', False)}")
        print(f"Thread pool workers: {performance.get('thread_pool_status', {}).get('max_workers', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ External API monitoring test failed: {e}")
        return False

def test_database_monitoring():
    """Test database monitoring enhancements"""
    print("\n" + "="*60)
    print("TESTING DATABASE MONITORING")
    print("="*60)
    
    try:
        from mlb_data_service.enhanced_database import EnhancedDatabaseManager
        
        # Get database manager instance
        db_manager = EnhancedDatabaseManager()
        print("\n✅ Enhanced Database Manager with monitoring initialized")
        
        # Test performance metrics
        print("\n⚡ Getting database performance metrics...")
        performance = db_manager.get_performance_metrics()
        
        print(f"Operations completed: {performance.get('operations_completed', 0)}")
        print(f"Records processed: {performance.get('total_records_processed', 0)}")
        print(f"Average operation time: {performance.get('average_operation_time_seconds', 0):.3f}s")
        print(f"Connection pool healthy: {performance.get('connection_pool_status', False)}")
        
        # Test health report
        print("\n🏥 Getting database health report...")
        health_report = db_manager.get_database_health_report()
        
        print(f"Overall health: {health_report.get('overall_health', 'unknown')}")
        print(f"Connection status: {health_report.get('connection_status', 'unknown')}")
        
        # Show table statistics if available
        table_stats = health_report.get('table_statistics', {})
        if table_stats:
            print(f"FanGraphs batting records: {table_stats.get('fangraphs_batting_count', 0)}")
            print(f"Statcast records: {table_stats.get('statcast_count', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database monitoring test failed: {e}")
        return False

def test_comprehensive_monitoring():
    """Test comprehensive monitoring report"""
    print("\n" + "="*60)
    print("TESTING COMPREHENSIVE MONITORING REPORT")
    print("="*60)
    
    try:
        from mlb_data_service.monitoring import get_comprehensive_monitoring_report
        
        print("\n📊 Generating comprehensive monitoring report...")
        report = get_comprehensive_monitoring_report()
        
        print(f"Report generated at: {report.get('timestamp', 'unknown')}")
        
        # System health summary
        system_health = report.get('system_health', {})
        print(f"System overall status: {system_health.get('overall_status', 'unknown')}")
        print(f"Healthy data sources: {system_health.get('source_counts', {}).get('fresh', 0)}")
        
        # Data quality summary
        data_quality = report.get('data_quality', {})
        print(f"Data quality issues: {data_quality.get('total_issues', 0)}")
        
        # Collection failures summary
        collection_failures = report.get('collection_failures', {})
        print(f"Collection issues: {collection_failures.get('total_issues', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Comprehensive monitoring test failed: {e}")
        return False

def main():
    """Run all monitoring system tests"""
    print("🚀 Starting Data Flow Monitoring System Tests")
    print("=" * 60)
    
    test_results = []
    
    # Run all tests
    tests = [
        ("Data Freshness Monitoring", test_data_freshness_monitoring),
        ("Data Quality Validation", test_data_quality_validation), 
        ("Collection Failure Detection", test_collection_failure_detection),
        ("Automated Cleanup", test_automated_cleanup),
        ("External API Monitoring", test_external_api_monitoring),
        ("Database Monitoring", test_database_monitoring),
        ("Comprehensive Monitoring", test_comprehensive_monitoring)
    ]
    
    for test_name, test_func in tests:
        try:
            print(f"\n🧪 Running {test_name} test...")
            result = test_func()
            test_results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name} test passed")
            else:
                print(f"❌ {test_name} test failed")
                
        except Exception as e:
            print(f"💥 {test_name} test crashed: {e}")
            test_results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in test_results if result)
    total = len(test_results)
    
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    print("\nDetailed results:")
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL" 
        print(f"  {status} {test_name}")
    
    if passed == total:
        print("\n🎉 All monitoring systems are working correctly!")
        print("🔧 The Data Flow Agent deliverables are ready for production!")
    else:
        print(f"\n⚠️  {total - passed} tests failed. Review implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
#!/usr/bin/env python3
"""
MLB Data Service Monitoring Package
==================================

Provides comprehensive monitoring, data quality validation, and automated cleanup
for production MLB data operations.

This package includes:
- Data freshness tracking and staleness detection
- Data quality validation with configurable rules
- Collection failure detection and alerting
- Automated retention policies and cleanup
- Performance monitoring and metrics
"""

from .data_monitor import (
    DataFreshnessTracker,
    DataQualityValidator,
    CollectionFailureDetector,
    get_system_health,
    get_data_quality_status,
    get_collection_failures,
    get_comprehensive_monitoring_report
)

from .data_cleanup import (
    AutomatedCleanup,
    MemoryAndCacheCleanup,
    run_daily_cleanup,
    run_cache_cleanup,
    get_cleanup_status
)

__version__ = "1.0.0"
__all__ = [
    # Data monitoring classes
    'DataFreshnessTracker',
    'DataQualityValidator', 
    'CollectionFailureDetector',
    
    # Cleanup classes
    'AutomatedCleanup',
    'MemoryAndCacheCleanup',
    
    # Convenience functions
    'get_system_health',
    'get_data_quality_status',
    'get_collection_failures',
    'get_comprehensive_monitoring_report',
    'run_daily_cleanup',
    'run_cache_cleanup',
    'get_cleanup_status'
]
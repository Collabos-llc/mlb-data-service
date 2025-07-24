#!/usr/bin/env python3
"""
Simple Test for Production Monitoring System
============================================

Basic functionality test for the monitoring components.
"""

import os
import sys
import logging
from datetime import datetime

# Add the mlb_data_service directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'mlb_data_service'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_health_monitor_basic():
    """Test basic health monitor functionality"""
    logger.info("Testing ProductionHealthMonitor basic functionality...")
    
    try:
        from monitoring.health_monitor import ProductionHealthMonitor
        
        # Create health monitor (without database for basic test)
        health_monitor = ProductionHealthMonitor(None)
        
        # Test system metrics collection
        system_metrics = health_monitor._get_system_metrics()
        logger.info(f"✅ System metrics collected:")
        logger.info(f"   CPU: {system_metrics.cpu_percent}%")
        logger.info(f"   Memory: {system_metrics.memory_percent}%")
        logger.info(f"   Disk: {system_metrics.disk_percent}%")
        logger.info(f"   Uptime: {system_metrics.uptime_seconds/3600:.1f} hours")
        
        # Stop monitoring
        health_monitor.stop_monitoring()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Health Monitor basic test failed: {e}")
        return False

def test_alert_manager_basic():
    """Test basic alert manager functionality"""
    logger.info("Testing AlertManager basic functionality...")
    
    try:
        from monitoring.alert_manager import AlertManager, AlertSeverity
        
        # Create alert manager (without database for basic test)
        alert_manager = AlertManager(None)
        
        # Create test alert
        alert_id = alert_manager.create_alert(
            name="Test Basic Alert",
            severity=AlertSeverity.WARNING,
            message="This is a basic test alert",
            source="basic_test",
            metric_value=42.0,
            threshold={"warning": 40, "critical": 80}
        )
        
        logger.info(f"✅ Created test alert: {alert_id}")
        
        # Test alert summary
        summary = alert_manager.get_alert_summary()
        logger.info(f"✅ Alert summary retrieved:")
        logger.info(f"   Total active: {summary.get('total_active')}")
        logger.info(f"   Critical: {summary.get('severity_breakdown', {}).get('critical')}")
        logger.info(f"   Warning: {summary.get('severity_breakdown', {}).get('warning')}")
        
        # Test acknowledgment
        ack_success = alert_manager.acknowledge_alert(alert_id, "test_user")
        logger.info(f"✅ Alert acknowledgment: {ack_success}")
        
        # Test resolution
        resolve_success = alert_manager.resolve_alert(alert_id, "Basic test completed")
        logger.info(f"✅ Alert resolution: {resolve_success}")
        
        # Stop processing
        alert_manager.stop_processing()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Alert Manager basic test failed: {e}")
        return False

def test_monitoring_imports():
    """Test that all monitoring modules can be imported"""
    logger.info("Testing monitoring module imports...")
    
    try:
        # Test imports
        from monitoring.health_monitor import ProductionHealthMonitor, HealthMetric, SystemMetrics
        from monitoring.alert_manager import AlertManager, Alert, AlertSeverity, AlertState
        from monitoring import ProductionHealthMonitor as PMH, AlertManager as AM
        
        logger.info("✅ All monitoring modules imported successfully")
        logger.info(f"   ProductionHealthMonitor: {ProductionHealthMonitor}")
        logger.info(f"   AlertManager: {AlertManager}")
        logger.info(f"   AlertSeverity enum: {list(AlertSeverity)}")
        logger.info(f"   AlertState enum: {list(AlertState)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Monitoring imports test failed: {e}")
        return False

def test_api_spec():
    """Test API specification compliance"""
    logger.info("Testing monitoring API specification...")
    
    try:
        # Define expected API endpoints
        expected_endpoints = [
            "/api/v1/health/detailed",
            "/api/v1/monitoring/alerts", 
            "/api/v1/monitoring/alerts/history",
            "/api/v1/monitoring/status",
            "/api/v1/monitoring/test-alert"
        ]
        
        # Read the enhanced_app.py file to verify endpoints exist
        app_file = os.path.join(os.path.dirname(__file__), 'mlb_data_service', 'enhanced_app.py')
        with open(app_file, 'r') as f:
            app_content = f.read()
        
        missing_endpoints = []
        for endpoint in expected_endpoints:
            if endpoint not in app_content:
                missing_endpoints.append(endpoint)
        
        if missing_endpoints:
            logger.error(f"❌ Missing API endpoints: {missing_endpoints}")
            return False
        
        logger.info("✅ All expected API endpoints found in enhanced_app.py")
        
        # Check for monitoring component initialization
        required_components = [
            "ProductionHealthMonitor",
            "AlertManager",
            "health_monitor = ProductionHealthMonitor",
            "alert_manager = AlertManager"
        ]
        
        missing_components = []
        for component in required_components:
            if component not in app_content:
                missing_components.append(component)
        
        if missing_components:
            logger.error(f"❌ Missing monitoring components: {missing_components}")
            return False
        
        logger.info("✅ All monitoring components properly integrated in Flask app")
        return True
        
    except Exception as e:
        logger.error(f"❌ API specification test failed: {e}")
        return False

def main():
    """Run basic monitoring system tests"""
    logger.info("🚀 Starting Basic Production Monitoring System Tests")
    logger.info("=" * 60)
    
    # Track test results
    tests = [
        ("Module Imports", test_monitoring_imports),
        ("API Specification", test_api_spec),
        ("Health Monitor Basic", test_health_monitor_basic),
        ("Alert Manager Basic", test_alert_manager_basic)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name} test...")
        logger.info("-" * 40)
        
        try:
            if test_func():
                passed += 1
                logger.info(f"✅ {test_name} test PASSED")
            else:
                logger.error(f"❌ {test_name} test FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} test FAILED with exception: {e}")
    
    # Final results
    logger.info("\n" + "=" * 60)
    logger.info(f"🏁 TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 ALL BASIC TESTS PASSED - Production monitoring system components ready!")
        return 0
    else:
        logger.error(f"💥 {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
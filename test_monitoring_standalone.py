#!/usr/bin/env python3
"""
Standalone Test for Production Monitoring System
===============================================

Test monitoring components without external dependencies.
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

def test_health_monitor_standalone():
    """Test health monitor without database dependency"""
    logger.info("Testing ProductionHealthMonitor standalone functionality...")
    
    try:
        # Import health monitor directly  
        from monitoring.health_monitor import ProductionHealthMonitor, SystemMetrics
        
        # Create health monitor without database
        health_monitor = ProductionHealthMonitor(database_manager=None)
        
        # Test system metrics collection
        system_metrics = health_monitor._get_system_metrics()
        logger.info(f"✅ System metrics collected:")
        logger.info(f"   CPU: {system_metrics.cpu_percent}%")
        logger.info(f"   Memory: {system_metrics.memory_percent}%")
        logger.info(f"   Disk: {system_metrics.disk_percent}%")
        logger.info(f"   Uptime: {system_metrics.uptime_seconds/3600:.1f} hours")
        logger.info(f"   Load Average: {system_metrics.load_average}")
        logger.info(f"   Network Connections: {system_metrics.network_connections}")
        
        # Test external API metrics (without actual API calls)
        api_metrics = health_monitor._get_external_api_metrics()
        logger.info(f"✅ API metrics collected:")
        logger.info(f"   PyBaseball: {api_metrics.pybaseball_status}")
        logger.info(f"   MLB API: {api_metrics.mlb_api_status}")
        logger.info(f"   FanGraphs: {api_metrics.fangraphs_status}")
        
        # Test threshold evaluation
        cpu_status = health_monitor._evaluate_threshold(75.0, {'warning': 70, 'critical': 90})
        logger.info(f"✅ Threshold evaluation (CPU 75%): {cpu_status}")
        
        # Stop monitoring
        health_monitor.stop_monitoring()
        
        logger.info("✅ Health Monitor standalone test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Health Monitor standalone test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_alert_manager_standalone():
    """Test alert manager without database dependency"""
    logger.info("Testing AlertManager standalone functionality...")
    
    try:
        from monitoring.alert_manager import AlertManager, AlertSeverity, AlertState
        
        # Create alert manager without database
        alert_manager = AlertManager(database_manager=None)
        
        # Create test alert
        alert_id = alert_manager.create_alert(
            name="Test Standalone Alert",
            severity=AlertSeverity.WARNING,
            message="This is a standalone test alert",
            source="standalone_test",
            metric_value=65.0,
            threshold={"warning": 60, "critical": 85}
        )
        
        logger.info(f"✅ Created test alert: {alert_id}")
        
        # Test alert summary
        summary = alert_manager.get_alert_summary()
        logger.info(f"✅ Alert summary retrieved:")
        logger.info(f"   Total active: {summary.get('total_active')}")
        logger.info(f"   Critical: {summary.get('severity_breakdown', {}).get('critical')}")
        logger.info(f"   Warning: {summary.get('severity_breakdown', {}).get('warning')}")
        logger.info(f"   Auto recovery: {summary.get('auto_recovery_enabled')}")
        
        # Test acknowledgment
        ack_success = alert_manager.acknowledge_alert(alert_id, "standalone_test_user")
        logger.info(f"✅ Alert acknowledgment: {ack_success}")
        
        # Test auto-recovery mechanism
        recovery_alerts = alert_manager.auto_resolve_if_recovered("standalone_test", "test_metric", 45.0)
        logger.info(f"✅ Auto-recovery check: {len(recovery_alerts)} alerts recovered")
        
        # Test resolution
        resolve_success = alert_manager.resolve_alert(alert_id, "Standalone test completed successfully")
        logger.info(f"✅ Alert resolution: {resolve_success}")
        
        # Test alert history (should work even without database backend)
        try:
            history = alert_manager.get_alert_history(hours=1, limit=10)
            logger.info(f"✅ Alert history retrieved: {len(history)} alerts")
        except Exception as e:
            logger.warning(f"Alert history failed (expected without database): {e}")
        
        # Stop processing
        alert_manager.stop_processing()
        
        logger.info("✅ Alert Manager standalone test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Alert Manager standalone test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_monitoring_integration():
    """Test monitoring system integration without external dependencies"""
    logger.info("Testing monitoring system integration...")
    
    try:
        from monitoring.health_monitor import ProductionHealthMonitor
        from monitoring.alert_manager import AlertManager, AlertSeverity
        
        # Initialize both components
        health_monitor = ProductionHealthMonitor(database_manager=None)
        alert_manager = AlertManager(database_manager=None)
        
        # Run health check
        health_data = health_monitor.run_health_check()
        logger.info(f"✅ Health check completed:")
        logger.info(f"   Overall Status: {health_data.get('overall_status')}")
        logger.info(f"   Service: {health_data.get('service')}")
        logger.info(f"   Uptime: {health_data.get('uptime_hours')} hours")
        
        # Check if any alerts should be created based on health data
        alerts_created = 0
        if health_data.get('alerts_triggered'):
            for alert_info in health_data.get('alerts_triggered', []):
                if 'Critical:' in alert_info:
                    alert_id = alert_manager.create_alert(
                        name="Health Integration Test",
                        severity=AlertSeverity.CRITICAL,
                        message=alert_info,
                        source='integration_test'
                    )
                    alerts_created += 1
                    logger.info(f"✅ Created alert from health check: {alert_id}")
        
        # Test dashboard-style data compilation
        dashboard_data = {
            "timestamp": datetime.now().isoformat(),
            "health": {
                "status": health_data.get('overall_status'),
                "metrics": health_data.get('metrics', {}),
                "recommendations": health_data.get('recommendations', [])
            },
            "alerts": alert_manager.get_alert_summary(),
            "uptime_hours": health_data.get('uptime_hours', 0)
        }
        
        logger.info(f"✅ Dashboard data compiled:")
        logger.info(f"   Health Status: {dashboard_data['health']['status']}")
        logger.info(f"   Active Alerts: {dashboard_data['alerts'].get('total_active', 0)}")
        logger.info(f"   System Uptime: {dashboard_data['uptime_hours']} hours")
        
        # Cleanup
        health_monitor.stop_monitoring()
        alert_manager.stop_processing()
        
        logger.info("✅ Monitoring integration test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Monitoring integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_configuration_and_thresholds():
    """Test configuration handling and threshold management"""
    logger.info("Testing configuration and thresholds...")
    
    try:
        from monitoring.health_monitor import ProductionHealthMonitor
        from monitoring.alert_manager import AlertManager
        
        # Test health monitor thresholds
        health_monitor = ProductionHealthMonitor(database_manager=None)
        
        # Test various threshold scenarios
        test_cases = [
            (50.0, {'warning': 70, 'critical': 90}, 'healthy'),
            (75.0, {'warning': 70, 'critical': 90}, 'warning'),
            (95.0, {'warning': 70, 'critical': 90}, 'critical'),
        ]
        
        for value, thresholds, expected in test_cases:
            result = health_monitor._evaluate_threshold(value, thresholds)
            if result == expected:
                logger.info(f"✅ Threshold test passed: {value}% -> {result}")
            else:
                logger.error(f"❌ Threshold test failed: {value}% -> {result} (expected {expected})")
                return False
        
        # Test alert manager configuration
        alert_manager = AlertManager(database_manager=None)
        
        # Verify default configuration
        config_checks = [
            ('auto_recovery_enabled', alert_manager.auto_recovery_enabled),
            ('notification_channels', len(alert_manager.notification_channels)),
            ('rate_limits', len(alert_manager.rate_limits)),
        ]
        
        for check_name, value in config_checks:
            logger.info(f"✅ Config check: {check_name} = {value}")
        
        # Cleanup
        health_monitor.stop_monitoring()
        alert_manager.stop_processing()
        
        logger.info("✅ Configuration and thresholds test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Configuration and thresholds test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run standalone monitoring system tests"""
    logger.info("🚀 Starting Standalone Production Monitoring System Tests")
    logger.info("=" * 65)
    
    # Track test results
    tests = [
        ("Health Monitor Standalone", test_health_monitor_standalone),
        ("Alert Manager Standalone", test_alert_manager_standalone),
        ("Monitoring Integration", test_monitoring_integration),
        ("Configuration & Thresholds", test_configuration_and_thresholds)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name} test...")
        logger.info("-" * 45)
        
        try:
            if test_func():
                passed += 1
                logger.info(f"✅ {test_name} test PASSED")
            else:
                logger.error(f"❌ {test_name} test FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} test FAILED with exception: {e}")
    
    # Final results
    logger.info("\n" + "=" * 65)
    logger.info(f"🏁 TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 ALL STANDALONE TESTS PASSED!")
        logger.info("Production monitoring system components are working correctly!")
        return 0
    else:
        logger.error(f"💥 {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
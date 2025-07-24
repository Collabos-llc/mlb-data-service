#!/usr/bin/env python3
"""
MLB Data Service - Monitoring Integration Tests
=============================================

Comprehensive end-to-end tests for the production monitoring stack.
Tests all components integration and failure scenarios.
"""

import requests
import time
import json
import subprocess
import psutil
import pytest
import logging
from datetime import datetime
from typing import Dict, List, Optional
import concurrent.futures
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MonitoringIntegrationTest:
    """Comprehensive monitoring integration test suite"""
    
    def __init__(self):
        self.services = {
            'mlb-data-service': 'http://localhost:8101',
            'prometheus': 'http://localhost:9090',
            'grafana': 'http://localhost:3000',
            'alertmanager': 'http://localhost:9093',
            'loki': 'http://localhost:3100'
        }
        self.test_results = {}
        self.start_time = datetime.now()
        
    def wait_for_services(self, timeout: int = 300) -> bool:
        """Wait for all services to be healthy"""
        logger.info("Waiting for services to be ready...")
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            all_ready = True
            for service_name, url in self.services.items():
                try:
                    if service_name == 'mlb-data-service':
                        response = requests.get(f"{url}/health", timeout=5)
                    elif service_name == 'prometheus':
                        response = requests.get(f"{url}/-/healthy", timeout=5)
                    elif service_name == 'grafana':
                        response = requests.get(f"{url}/api/health", timeout=5)
                    elif service_name == 'alertmanager':
                        response = requests.get(f"{url}/-/healthy", timeout=5)
                    elif service_name == 'loki':
                        response = requests.get(f"{url}/ready", timeout=5)
                    
                    if response.status_code != 200:
                        all_ready = False
                        break
                        
                except requests.RequestException:
                    all_ready = False
                    break
            
            if all_ready:
                logger.info("All services are ready!")
                return True
                
            logger.info("Services not ready yet, waiting...")
            time.sleep(10)
        
        logger.error("Timeout waiting for services to be ready")
        return False

    def test_service_health_checks(self) -> Dict[str, bool]:
        """Test health checks for all services"""
        logger.info("Testing service health checks...")
        results = {}
        
        # Test MLB Data Service health
        try:
            response = requests.get(f"{self.services['mlb-data-service']}/health", timeout=10)
            health_data = response.json()
            results['mlb-service-health'] = (
                response.status_code == 200 and 
                health_data.get('status') == 'healthy' and
                health_data.get('database') == 'connected'
            )
            logger.info(f"MLB Service Health: {results['mlb-service-health']}")
        except Exception as e:
            logger.error(f"MLB Service health check failed: {e}")
            results['mlb-service-health'] = False
        
        # Test Prometheus health
        try:
            response = requests.get(f"{self.services['prometheus']}/-/healthy", timeout=10)
            results['prometheus-health'] = response.status_code == 200
            logger.info(f"Prometheus Health: {results['prometheus-health']}")
        except Exception as e:
            logger.error(f"Prometheus health check failed: {e}")
            results['prometheus-health'] = False
            
        # Test Grafana health
        try:
            response = requests.get(f"{self.services['grafana']}/api/health", timeout=10)
            results['grafana-health'] = response.status_code == 200
            logger.info(f"Grafana Health: {results['grafana-health']}")
        except Exception as e:
            logger.error(f"Grafana health check failed: {e}")
            results['grafana-health'] = False
            
        # Test Alertmanager health
        try:
            response = requests.get(f"{self.services['alertmanager']}/-/healthy", timeout=10)
            results['alertmanager-health'] = response.status_code == 200
            logger.info(f"Alertmanager Health: {results['alertmanager-health']}")
        except Exception as e:
            logger.error(f"Alertmanager health check failed: {e}")
            results['alertmanager-health'] = False
            
        # Test Loki health
        try:
            response = requests.get(f"{self.services['loki']}/ready", timeout=10)
            results['loki-health'] = response.status_code == 200
            logger.info(f"Loki Health: {results['loki-health']}")
        except Exception as e:
            logger.error(f"Loki health check failed: {e}")
            results['loki-health'] = False
        
        return results

    def test_metrics_collection(self) -> Dict[str, bool]:
        """Test metrics collection and scraping"""
        logger.info("Testing metrics collection...")
        results = {}
        
        # Test service metrics endpoint
        try:
            response = requests.get(f"{self.services['mlb-data-service']}/metrics", timeout=10)
            results['service-metrics'] = response.status_code == 200 and 'http_requests_total' in response.text
            logger.info(f"Service Metrics Collection: {results['service-metrics']}")
        except Exception as e:
            logger.error(f"Service metrics test failed: {e}")
            results['service-metrics'] = False
        
        # Test Prometheus scraping
        try:
            time.sleep(30)  # Wait for scraping
            response = requests.get(
                f"{self.services['prometheus']}/api/v1/query",
                params={'query': 'up{job="mlb-data-service"}'},
                timeout=10
            )
            prom_data = response.json()
            results['prometheus-scraping'] = (
                response.status_code == 200 and 
                prom_data.get('status') == 'success' and
                len(prom_data.get('data', {}).get('result', [])) > 0
            )
            logger.info(f"Prometheus Scraping: {results['prometheus-scraping']}")
        except Exception as e:
            logger.error(f"Prometheus scraping test failed: {e}")
            results['prometheus-scraping'] = False
        
        return results

    def test_alerting_system(self) -> Dict[str, bool]:
        """Test alerting configuration and rules"""
        logger.info("Testing alerting system...")
        results = {}
        
        # Test alert rules loading
        try:
            response = requests.get(f"{self.services['prometheus']}/api/v1/rules", timeout=10)
            rules_data = response.json()
            results['alert-rules'] = (
                response.status_code == 200 and 
                rules_data.get('status') == 'success' and
                len(rules_data.get('data', {}).get('groups', [])) > 0
            )
            logger.info(f"Alert Rules Loading: {results['alert-rules']}")
        except Exception as e:
            logger.error(f"Alert rules test failed: {e}")
            results['alert-rules'] = False
        
        # Test Alertmanager configuration
        try:
            response = requests.get(f"{self.services['alertmanager']}/api/v1/status", timeout=10)
            results['alertmanager-config'] = response.status_code == 200
            logger.info(f"Alertmanager Config: {results['alertmanager-config']}")
        except Exception as e:
            logger.error(f"Alertmanager config test failed: {e}")
            results['alertmanager-config'] = False
        
        return results

    def test_logging_integration(self) -> Dict[str, bool]:
        """Test log aggregation with Loki"""
        logger.info("Testing logging integration...")
        results = {}
        
        # Generate some log entries
        try:
            # Make requests to generate logs
            for i in range(5):
                requests.get(f"{self.services['mlb-data-service']}/api/v1/status", timeout=5)
                time.sleep(1)
        except Exception as e:
            logger.warning(f"Could not generate test logs: {e}")
        
        # Wait for log ingestion
        time.sleep(30)
        
        # Test Loki log ingestion
        try:
            response = requests.get(
                f"{self.services['loki']}/loki/api/v1/label",
                timeout=10
            )
            results['loki-ingestion'] = response.status_code == 200
            logger.info(f"Loki Log Ingestion: {results['loki-ingestion']}")
        except Exception as e:
            logger.error(f"Loki ingestion test failed: {e}")
            results['loki-ingestion'] = False
        
        return results

    def test_grafana_integration(self) -> Dict[str, bool]:
        """Test Grafana dashboard and datasource integration"""
        logger.info("Testing Grafana integration...")
        results = {}
        
        # Test datasources
        try:
            response = requests.get(
                f"{self.services['grafana']}/api/datasources",
                timeout=10,
                auth=('admin', 'admin123')
            )
            datasources = response.json()
            prometheus_ds = any(ds.get('type') == 'prometheus' for ds in datasources)
            loki_ds = any(ds.get('type') == 'loki' for ds in datasources)
            
            results['grafana-datasources'] = response.status_code == 200 and prometheus_ds and loki_ds
            logger.info(f"Grafana Datasources: {results['grafana-datasources']}")
        except Exception as e:
            logger.error(f"Grafana datasources test failed: {e}")
            results['grafana-datasources'] = False
        
        # Test dashboard access
        try:
            response = requests.get(
                f"{self.services['grafana']}/api/search",
                timeout=10,
                auth=('admin', 'admin123')
            )
            results['grafana-dashboards'] = response.status_code == 200
            logger.info(f"Grafana Dashboards: {results['grafana-dashboards']}")
        except Exception as e:
            logger.error(f"Grafana dashboards test failed: {e}")
            results['grafana-dashboards'] = False
        
        return results

    def test_load_scenario(self) -> Dict[str, bool]:
        """Test monitoring under load conditions"""
        logger.info("Testing monitoring under load...")
        results = {}
        
        def make_requests():
            """Generate load by making requests"""
            for i in range(50):
                try:
                    requests.get(f"{self.services['mlb-data-service']}/api/v1/status", timeout=5)
                    time.sleep(0.1)
                except Exception:
                    pass
        
        # Generate load with multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=make_requests)
            threads.append(thread)
            thread.start()
        
        # Wait for load generation to complete
        for thread in threads:
            thread.join()
        
        # Wait for metrics to be scraped
        time.sleep(60)
        
        # Check if metrics reflect the load
        try:
            response = requests.get(
                f"{self.services['prometheus']}/api/v1/query",
                params={'query': 'rate(http_requests_total{job="mlb-data-service"}[5m])'},
                timeout=10
            )
            query_data = response.json()
            
            if (response.status_code == 200 and 
                query_data.get('status') == 'success' and
                query_data.get('data', {}).get('result')):
                
                # Check if rate is greater than 0
                rate_value = float(query_data['data']['result'][0]['value'][1])
                results['load-metrics'] = rate_value > 0
            else:
                results['load-metrics'] = False
                
            logger.info(f"Load Test Metrics: {results['load-metrics']}")
        except Exception as e:
            logger.error(f"Load test failed: {e}")
            results['load-metrics'] = False
        
        return results

    def test_failure_scenarios(self) -> Dict[str, bool]:
        """Test monitoring behavior during failure scenarios"""
        logger.info("Testing failure scenarios...")
        results = {}
        
        # Test 404 endpoint to generate errors
        try:
            for i in range(10):
                requests.get(f"{self.services['mlb-data-service']}/nonexistent", timeout=5)
            
            # Wait for metrics
            time.sleep(30)
            
            # Check if error metrics are captured
            response = requests.get(
                f"{self.services['prometheus']}/api/v1/query",
                params={'query': 'http_requests_total{job="mlb-data-service",status="404"}'},
                timeout=10
            )
            query_data = response.json()
            
            results['error-monitoring'] = (
                response.status_code == 200 and
                query_data.get('status') == 'success' and
                len(query_data.get('data', {}).get('result', [])) > 0
            )
            logger.info(f"Error Monitoring: {results['error-monitoring']}")
        except Exception as e:
            logger.error(f"Error monitoring test failed: {e}")
            results['error-monitoring'] = False
        
        return results

    def test_data_collection_monitoring(self) -> Dict[str, bool]:
        """Test monitoring of data collection processes"""
        logger.info("Testing data collection monitoring...")
        results = {}
        
        # Trigger data collection
        try:
            response = requests.post(
                f"{self.services['mlb-data-service']}/api/v1/collect/players",
                json={'limit': 5},
                timeout=60
            )
            
            results['data-collection-trigger'] = response.status_code == 200
            logger.info(f"Data Collection Trigger: {results['data-collection-trigger']}")
            
            # Wait for collection to complete and metrics to be scraped
            time.sleep(60)
            
            # Check collection metrics
            metrics_response = requests.get(
                f"{self.services['mlb-data-service']}/metrics",
                timeout=10
            )
            
            results['collection-metrics'] = (
                metrics_response.status_code == 200 and
                'data_collection' in metrics_response.text
            )
            logger.info(f"Collection Metrics: {results['collection-metrics']}")
            
        except Exception as e:
            logger.error(f"Data collection monitoring test failed: {e}")
            results['data-collection-trigger'] = False
            results['collection-metrics'] = False
        
        return results

    def generate_comprehensive_report(self) -> Dict[str, any]:
        """Generate comprehensive test report"""
        logger.info("Generating comprehensive test report...")
        
        all_results = {}
        
        # Run all test suites
        test_suites = [
            ('Health Checks', self.test_service_health_checks),
            ('Metrics Collection', self.test_metrics_collection),
            ('Alerting System', self.test_alerting_system),
            ('Logging Integration', self.test_logging_integration),
            ('Grafana Integration', self.test_grafana_integration),
            ('Load Testing', self.test_load_scenario),
            ('Failure Scenarios', self.test_failure_scenarios),
            ('Data Collection Monitoring', self.test_data_collection_monitoring)
        ]
        
        for suite_name, test_func in test_suites:
            logger.info(f"Running {suite_name} tests...")
            try:
                suite_results = test_func()
                all_results[suite_name] = suite_results
            except Exception as e:
                logger.error(f"Test suite {suite_name} failed: {e}")
                all_results[suite_name] = {'error': str(e)}
        
        # Calculate overall statistics
        total_tests = sum(len(suite) for suite in all_results.values() if isinstance(suite, dict))
        passed_tests = sum(
            sum(1 for result in suite.values() if result is True)
            for suite in all_results.values()
            if isinstance(suite, dict)
        )
        
        # Generate summary
        summary = {
            'test_run_time': datetime.now().isoformat(),
            'total_duration': str(datetime.now() - self.start_time),
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': total_tests - passed_tests,
            'success_rate': f"{(passed_tests / total_tests * 100):.1f}%" if total_tests > 0 else "0%",
            'overall_status': 'PASS' if passed_tests == total_tests else 'FAIL'
        }
        
        return {
            'summary': summary,
            'detailed_results': all_results,
            'recommendations': self.generate_recommendations(all_results)
        }

    def generate_recommendations(self, results: Dict[str, any]) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []
        
        for suite_name, suite_results in results.items():
            if isinstance(suite_results, dict):
                for test_name, result in suite_results.items():
                    if result is False:
                        if 'health' in test_name.lower():
                            recommendations.append(f"Check {test_name} - service may not be running or configured correctly")
                        elif 'metrics' in test_name.lower():
                            recommendations.append(f"Review metrics configuration for {test_name}")
                        elif 'alert' in test_name.lower():
                            recommendations.append(f"Verify alerting setup for {test_name}")
                        elif 'grafana' in test_name.lower():
                            recommendations.append(f"Check Grafana configuration for {test_name}")
                        elif 'loki' in test_name.lower():
                            recommendations.append(f"Review log aggregation setup for {test_name}")
        
        if not recommendations:
            recommendations.append("All tests passed! Monitoring stack is functioning correctly.")
        
        return recommendations


def run_monitoring_integration_tests():
    """Main function to run all monitoring integration tests"""
    print("=" * 60)
    print("MLB Data Service - Monitoring Integration Tests")
    print("=" * 60)
    
    test_runner = MonitoringIntegrationTest()
    
    # Wait for services to be ready
    if not test_runner.wait_for_services():
        print("FATAL: Services are not ready. Cannot proceed with tests.")
        return False
    
    # Run comprehensive tests
    report = test_runner.generate_comprehensive_report()
    
    # Print results
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    summary = report['summary']
    print(f"Test Run Time: {summary['test_run_time']}")
    print(f"Total Duration: {summary['total_duration']}")
    print(f"Total Tests: {summary['total_tests']}")
    print(f"Passed: {summary['passed_tests']}")
    print(f"Failed: {summary['failed_tests']}")
    print(f"Success Rate: {summary['success_rate']}")
    print(f"Overall Status: {summary['overall_status']}")
    
    print("\n" + "=" * 60)
    print("DETAILED RESULTS")
    print("=" * 60)
    
    for suite_name, suite_results in report['detailed_results'].items():
        print(f"\n{suite_name}:")
        if isinstance(suite_results, dict):
            for test_name, result in suite_results.items():
                status = "✅ PASS" if result is True else "❌ FAIL" if result is False else f"⚠️  {result}"
                print(f"  {test_name}: {status}")
        else:
            print(f"  Error: {suite_results}")
    
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    
    for i, recommendation in enumerate(report['recommendations'], 1):
        print(f"{i}. {recommendation}")
    
    # Save detailed report
    with open('/tmp/monitoring_integration_test_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nDetailed report saved to: /tmp/monitoring_integration_test_report.json")
    
    return summary['overall_status'] == 'PASS'


if __name__ == "__main__":
    success = run_monitoring_integration_tests()
    exit(0 if success else 1)
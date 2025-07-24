#!/usr/bin/env python3
"""
MLB Data Service - Production Deployment Validation
==================================================

Validates that the production deployment is ready and all components
are correctly configured for production use.
"""

import subprocess
import yaml
import json
import os
import sys
import time
from typing import Dict, List, Tuple
import requests
from pathlib import Path

class ProductionValidator:
    """Validates production deployment configuration and readiness"""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.validation_results = {}
        
    def validate_configuration_files(self) -> Dict[str, bool]:
        """Validate all configuration files exist and are valid"""
        results = {}
        
        # Required configuration files
        config_files = {
            'docker-compose.yml': self.project_root / 'docker-compose.yml',
            'production.yaml': self.project_root / 'config' / 'production.yaml',
            'monitoring-config.json': self.project_root / 'config' / 'monitoring-config.json',
            'prometheus.yml': self.project_root / 'config' / 'prometheus.yml',
            'alertmanager.yml': self.project_root / 'config' / 'alertmanager.yml',
            'loki.yml': self.project_root / 'config' / 'loki.yml',
            'promtail.yml': self.project_root / 'config' / 'promtail.yml',
            '.env.production': self.project_root / '.env.production'
        }
        
        for file_name, file_path in config_files.items():
            try:
                if file_path.exists() and file_path.stat().st_size > 0:
                    # Try to parse YAML/JSON files
                    if file_name.endswith('.yml') or file_name.endswith('.yaml'):
                        with open(file_path, 'r') as f:
                            yaml.safe_load(f)
                    elif file_name.endswith('.json'):
                        with open(file_path, 'r') as f:
                            json.load(f)
                    
                    results[file_name] = True
                    print(f"✅ {file_name}: Valid")
                else:
                    results[file_name] = False
                    print(f"❌ {file_name}: Missing or empty")
                    
            except Exception as e:
                results[file_name] = False
                print(f"❌ {file_name}: Invalid format - {e}")
        
        return results
    
    def validate_docker_configuration(self) -> Dict[str, bool]:
        """Validate Docker configuration for production"""
        results = {}
        
        try:
            with open(self.project_root / 'docker-compose.yml', 'r') as f:
                compose_config = yaml.safe_load(f)
            
            # Check if monitoring services are defined
            services = compose_config.get('services', {})
            required_services = [
                'postgres', 'mlb-data-service', 'prometheus', 
                'grafana', 'alertmanager', 'node-exporter', 'loki', 'promtail'
            ]
            
            for service in required_services:
                results[f'service_{service}'] = service in services
                status = "✅" if service in services else "❌"
                print(f"{status} Service {service}: {'Configured' if service in services else 'Missing'}")
            
            # Check for production configurations
            mlb_service = services.get('mlb-data-service', {})
            environment = mlb_service.get('environment', {})
            
            # Convert list of env vars to dict for easier checking
            if isinstance(environment, list):
                env_dict = {}
                for env_var in environment:
                    if '=' in env_var:
                        key, value = env_var.split('=', 1)
                        env_dict[key] = value
                    else:
                        # Variable without value (e.g., just key)
                        env_dict[env_var] = None
                environment = env_dict
            
            results['production_env'] = environment.get('FLASK_ENV', '').endswith('production}') or 'production' in str(environment.get('FLASK_ENV', ''))
            results['monitoring_enabled'] = environment.get('MONITORING_ENABLED', '').endswith('true}') or 'true' in str(environment.get('MONITORING_ENABLED', ''))
            
            # Check for health checks
            results['health_checks'] = 'healthcheck' in mlb_service
            
            # Check for restart policies
            results['restart_policy'] = mlb_service.get('restart') == 'unless-stopped'
            
            # Check for resource limits
            deploy_config = mlb_service.get('deploy', {})
            resources = deploy_config.get('resources', {})
            results['resource_limits'] = 'limits' in resources
            
            print(f"✅ Production Environment: {results['production_env']}")
            print(f"✅ Monitoring Enabled: {results['monitoring_enabled']}")
            print(f"✅ Health Checks: {results['health_checks']}")
            print(f"✅ Restart Policy: {results['restart_policy']}")
            print(f"✅ Resource Limits: {results['resource_limits']}")
            
        except Exception as e:
            print(f"❌ Docker configuration validation failed: {e}")
            results['docker_config_error'] = str(e)
        
        return results
    
    def validate_monitoring_configuration(self) -> Dict[str, bool]:
        """Validate monitoring stack configuration"""
        results = {}
        
        try:
            # Validate Prometheus configuration
            with open(self.project_root / 'config' / 'prometheus.yml', 'r') as f:
                prom_config = yaml.safe_load(f)
            
            scrape_configs = prom_config.get('scrape_configs', [])
            job_names = [job.get('job_name') for job in scrape_configs]
            
            required_jobs = ['mlb-data-service', 'prometheus', 'node-exporter']
            for job in required_jobs:
                results[f'prometheus_job_{job}'] = job in job_names
            
            results['alerting_configured'] = 'alerting' in prom_config
            results['rule_files_configured'] = len(prom_config.get('rule_files', [])) > 0
            
            # Validate Alertmanager configuration
            with open(self.project_root / 'config' / 'alertmanager.yml', 'r') as f:
                alert_config = yaml.safe_load(f)
            
            results['alertmanager_routes'] = 'route' in alert_config
            results['alertmanager_receivers'] = len(alert_config.get('receivers', [])) > 0
            
            # Validate Grafana datasources
            grafana_ds_file = self.project_root / 'config' / 'grafana' / 'provisioning' / 'datasources' / 'datasources.yml'
            if grafana_ds_file.exists():
                with open(grafana_ds_file, 'r') as f:
                    grafana_config = yaml.safe_load(f)
                
                datasources = grafana_config.get('datasources', [])
                ds_types = [ds.get('type') for ds in datasources]
                
                results['grafana_prometheus_ds'] = 'prometheus' in ds_types
                results['grafana_loki_ds'] = 'loki' in ds_types
            else:
                results['grafana_prometheus_ds'] = False
                results['grafana_loki_ds'] = False
            
            print(f"✅ Prometheus Jobs: {sum(1 for k, v in results.items() if k.startswith('prometheus_job') and v)}/{len(required_jobs)}")
            print(f"✅ Alerting Configured: {results['alerting_configured']}")
            print(f"✅ Alert Rules: {results['rule_files_configured']}")
            print(f"✅ Alertmanager Routes: {results['alertmanager_routes']}")
            print(f"✅ Grafana Datasources: Prometheus={results['grafana_prometheus_ds']}, Loki={results['grafana_loki_ds']}")
            
        except Exception as e:
            print(f"❌ Monitoring configuration validation failed: {e}")
            results['monitoring_config_error'] = str(e)
        
        return results
    
    def validate_network_configuration(self) -> Dict[str, bool]:
        """Validate network and port configuration"""
        results = {}
        
        try:
            with open(self.project_root / 'docker-compose.yml', 'r') as f:
                compose_config = yaml.safe_load(f)
            
            services = compose_config.get('services', {})
            
            # Check port configurations
            expected_ports = {
                'mlb-data-service': '8101',
                'prometheus': '9090', 
                'grafana': '3000',
                'alertmanager': '9093',
                'node-exporter': '9100',
                'loki': '3100'
            }
            
            for service_name, expected_port in expected_ports.items():
                service_config = services.get(service_name, {})
                ports = service_config.get('ports', [])
                
                # Check if expected port is configured
                port_configured = any(expected_port in str(port) for port in ports)
                results[f'port_{service_name}'] = port_configured
                
                status = "✅" if port_configured else "❌"
                print(f"{status} Port {service_name}: {expected_port}")
            
            # Check network configuration
            networks = compose_config.get('networks', {})
            results['mlb_network'] = 'mlb-network' in networks
            
            # Verify all services are on the same network
            services_on_network = 0
            for service_config in services.values():
                if 'mlb-network' in service_config.get('networks', []):
                    services_on_network += 1
            
            results['services_networked'] = services_on_network >= len(expected_ports)
            
            print(f"✅ Network Configuration: {results['mlb_network']}")
            print(f"✅ Services Networked: {services_on_network}/{len(expected_ports)}")
            
        except Exception as e:
            print(f"❌ Network configuration validation failed: {e}")
            results['network_config_error'] = str(e)
        
        return results
    
    def validate_security_configuration(self) -> Dict[str, bool]:
        """Validate security configurations"""
        results = {}
        
        try:
            # Check environment file for security configurations
            env_file = self.project_root / '.env.production'
            if env_file.exists():
                with open(env_file, 'r') as f:
                    env_content = f.read()
                
                # Check for password configurations
                results['postgres_password'] = 'POSTGRES_PASSWORD=' in env_content
                results['grafana_password'] = 'GRAFANA_ADMIN_PASSWORD=' in env_content
                
                # Check for alert configurations
                results['alert_webhook'] = 'ALERT_WEBHOOK_URL=' in env_content
                
                print(f"✅ Postgres Password: {results['postgres_password']}")
                print(f"✅ Grafana Password: {results['grafana_password']}")
                print(f"✅ Alert Webhook: {results['alert_webhook']}")
            else:
                results['env_file_missing'] = True
                print("❌ .env.production file missing")
            
            # Check Docker security configurations
            with open(self.project_root / 'docker-compose.yml', 'r') as f:
                compose_config = yaml.safe_load(f)
            
            services = compose_config.get('services', {})
            
            # Check restart policies
            services_with_restart = sum(
                1 for service in services.values() 
                if service.get('restart') in ['unless-stopped', 'always']
            )
            results['restart_policies'] = services_with_restart >= 5
            
            print(f"✅ Restart Policies: {services_with_restart}/8 services configured")
            
        except Exception as e:
            print(f"❌ Security configuration validation failed: {e}")
            results['security_config_error'] = str(e)
        
        return results
    
    def validate_system_requirements(self) -> Dict[str, bool]:
        """Validate system requirements for production deployment"""
        results = {}
        
        try:
            # Check Docker and Docker Compose
            docker_result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
            results['docker_installed'] = docker_result.returncode == 0
            
            compose_result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
            results['docker_compose_installed'] = compose_result.returncode == 0
            
            print(f"✅ Docker: {results['docker_installed']}")
            print(f"✅ Docker Compose: {results['docker_compose_installed']}")
            
            # Check required directories
            required_dirs = [
                self.project_root / 'config',
                self.project_root / 'logs',
                self.project_root / 'config' / 'grafana',
                self.project_root / 'config' / 'grafana' / 'dashboards',
                self.project_root / 'config' / 'grafana' / 'provisioning'
            ]
            
            for dir_path in required_dirs:
                results[f'dir_{dir_path.name}'] = dir_path.exists()
                status = "✅" if dir_path.exists() else "❌"
                print(f"{status} Directory {dir_path.name}: {'Exists' if dir_path.exists() else 'Missing'}")
            
        except Exception as e:
            print(f"❌ System requirements validation failed: {e}")
            results['system_requirements_error'] = str(e)
        
        return results
    
    def generate_validation_report(self) -> Dict[str, any]:
        """Generate comprehensive validation report"""
        print("=" * 60)
        print("MLB Data Service - Production Deployment Validation")
        print("=" * 60)
        
        validation_suites = [
            ('Configuration Files', self.validate_configuration_files),
            ('Docker Configuration', self.validate_docker_configuration),
            ('Monitoring Configuration', self.validate_monitoring_configuration),
            ('Network Configuration', self.validate_network_configuration),
            ('Security Configuration', self.validate_security_configuration),
            ('System Requirements', self.validate_system_requirements)
        ]
        
        all_results = {}
        
        for suite_name, validation_func in validation_suites:
            print(f"\n{suite_name}:")
            print("-" * 40)
            try:
                suite_results = validation_func()
                all_results[suite_name] = suite_results
            except Exception as e:
                print(f"❌ Validation suite failed: {e}")
                all_results[suite_name] = {'error': str(e)}
        
        # Calculate statistics
        total_checks = sum(
            len(suite) for suite in all_results.values() 
            if isinstance(suite, dict) and 'error' not in suite
        )
        
        passed_checks = sum(
            sum(1 for result in suite.values() if result is True)
            for suite in all_results.values()
            if isinstance(suite, dict) and 'error' not in suite
        )
        
        success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        # Generate recommendations
        recommendations = []
        for suite_name, suite_results in all_results.items():
            if isinstance(suite_results, dict) and 'error' not in suite_results:
                for check_name, result in suite_results.items():
                    if result is False:
                        recommendations.append(f"Fix {check_name} in {suite_name}")
        
        if not recommendations:
            recommendations.append("All validation checks passed! Deployment is ready for production.")
        
        summary = {
            'validation_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': total_checks - passed_checks,
            'success_rate': f"{success_rate:.1f}%",
            'overall_status': 'READY' if passed_checks == total_checks else 'NOT_READY'
        }
        
        return {
            'summary': summary,
            'detailed_results': all_results,
            'recommendations': recommendations
        }


def main():
    """Main validation function"""
    project_root = '/home/jeffreyconboy/github-repos/mlb-data-service'
    
    validator = ProductionValidator(project_root)
    report = validator.generate_validation_report()
    
    # Print summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    summary = report['summary']
    print(f"Validation Time: {summary['validation_time']}")
    print(f"Total Checks: {summary['total_checks']}")
    print(f"Passed: {summary['passed_checks']}")
    print(f"Failed: {summary['failed_checks']}")
    print(f"Success Rate: {summary['success_rate']}")
    print(f"Overall Status: {summary['overall_status']}")
    
    # Print recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    
    for i, recommendation in enumerate(report['recommendations'], 1):
        print(f"{i}. {recommendation}")
    
    # Save report
    report_file = '/tmp/production_deployment_validation.json'
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nDetailed report saved to: {report_file}")
    
    return summary['overall_status'] == 'READY'


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
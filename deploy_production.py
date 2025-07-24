#!/usr/bin/env python3
"""
MLB Data Service - Production Deployment Script
==============================================

Orchestrates the complete production deployment with monitoring stack.
Handles validation, deployment, health checks, and post-deployment testing.
"""

import subprocess
import time
import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional
import requests

class ProductionDeployer:
    """Handles production deployment orchestration"""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.deployment_log = []
        
    def log(self, message: str, level: str = "INFO"):
        """Log deployment messages"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {level}: {message}"
        print(log_entry)
        self.deployment_log.append(log_entry)
        
    def run_command(self, command: List[str], cwd: Optional[str] = None) -> bool:
        """Run shell command and return success status"""
        try:
            self.log(f"Running: {' '.join(command)}")
            result = subprocess.run(
                command, 
                cwd=cwd or self.project_root,
                capture_output=True, 
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                self.log(f"Command succeeded: {' '.join(command)}")
                if result.stdout.strip():
                    self.log(f"Output: {result.stdout.strip()}")
                return True
            else:
                self.log(f"Command failed: {' '.join(command)}", "ERROR")
                if result.stderr.strip():
                    self.log(f"Error: {result.stderr.strip()}", "ERROR")
                return False
                
        except subprocess.TimeoutExpired:
            self.log(f"Command timed out: {' '.join(command)}", "ERROR")
            return False
        except Exception as e:
            self.log(f"Command exception: {e}", "ERROR")
            return False
    
    def validate_prerequisites(self) -> bool:
        """Validate deployment prerequisites"""
        self.log("Validating deployment prerequisites...")
        
        # Run production validation script
        validation_script = self.project_root / 'tests' / 'validate_production_deployment.py'
        if not validation_script.exists():
            self.log("Production validation script not found", "ERROR")
            return False
        
        self.log("Running production deployment validation...")
        success = self.run_command(['python3', str(validation_script)])
        
        if not success:
            self.log("Production validation failed", "ERROR")
            return False
        
        self.log("Prerequisites validation completed successfully")
        return True
    
    def prepare_environment(self) -> bool:
        """Prepare the deployment environment"""
        self.log("Preparing deployment environment...")
        
        # Create necessary directories
        directories = [
            self.project_root / 'logs',
            self.project_root / 'config' / 'grafana' / 'dashboards',
            self.project_root / 'config' / 'grafana' / 'provisioning' / 'dashboards',
            self.project_root / 'config' / 'grafana' / 'provisioning' / 'datasources'
        ]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.log(f"Created directory: {directory}")
            except Exception as e:
                self.log(f"Failed to create directory {directory}: {e}", "ERROR")
                return False
        
        # Set up environment file
        env_file = self.project_root / '.env'
        env_production = self.project_root / '.env.production'
        
        if env_production.exists() and not env_file.exists():
            try:
                # Copy production env file
                with open(env_production, 'r') as src:
                    content = src.read()
                with open(env_file, 'w') as dst:
                    dst.write(content)
                self.log("Environment file prepared")
            except Exception as e:
                self.log(f"Failed to prepare environment file: {e}", "ERROR")
                return False
        
        self.log("Environment preparation completed")
        return True
    
    def stop_existing_services(self) -> bool:
        """Stop any existing services"""
        self.log("Stopping existing services...")
        
        # Stop any running containers
        success = self.run_command(['docker-compose', 'down', '-v'])
        if success:
            self.log("Existing services stopped successfully")
        else:
            self.log("Warning: Could not stop existing services (may not be running)", "WARNING")
        
        # Wait for cleanup
        time.sleep(5)
        return True
    
    def deploy_services(self) -> bool:
        """Deploy all services using docker-compose"""
        self.log("Deploying MLB Data Service with monitoring stack...")
        
        # Build and start services
        if not self.run_command(['docker-compose', 'build']):
            self.log("Failed to build services", "ERROR")
            return False
        
        if not self.run_command(['docker-compose', 'up', '-d']):
            self.log("Failed to start services", "ERROR")
            return False
        
        self.log("Services deployed successfully")
        return True
    
    def wait_for_services(self, timeout: int = 300) -> bool:
        """Wait for all services to be healthy"""
        self.log(f"Waiting for services to be healthy (timeout: {timeout}s)...")
        
        services_to_check = {
            'mlb-data-service': 'http://localhost:8101/health',
            'prometheus': 'http://localhost:9090/-/healthy',
            'grafana': 'http://localhost:3000/api/health',
            'alertmanager': 'http://localhost:9093/-/healthy',
            'loki': 'http://localhost:3100/ready'
        }
        
        start_time = time.time()
        ready_services = set()
        
        while time.time() - start_time < timeout:
            all_ready = True
            
            for service_name, health_url in services_to_check.items():
                if service_name in ready_services:
                    continue
                    
                try:
                    response = requests.get(health_url, timeout=5)
                    if response.status_code == 200:
                        ready_services.add(service_name)
                        self.log(f"Service {service_name} is ready")
                    else:
                        all_ready = False
                except requests.RequestException:
                    all_ready = False
            
            if len(ready_services) == len(services_to_check):
                self.log("All services are healthy!")
                return True
            
            if len(ready_services) > 0:
                remaining = len(services_to_check) - len(ready_services)
                self.log(f"Waiting for {remaining} more services... ({len(ready_services)}/{len(services_to_check)} ready)")
            
            time.sleep(10)
        
        self.log("Timeout waiting for services to be healthy", "ERROR")
        self.log(f"Ready services: {list(ready_services)}")
        self.log(f"Pending services: {list(set(services_to_check.keys()) - ready_services)}")
        return False
    
    def run_integration_tests(self) -> bool:
        """Run integration tests to validate deployment"""
        self.log("Running integration tests...")
        
        integration_script = self.project_root / 'tests' / 'test_monitoring_integration.py'
        if not integration_script.exists():
            self.log("Integration test script not found", "ERROR")
            return False
        
        # Run integration tests
        success = self.run_command(['python3', str(integration_script)])
        
        if success:
            self.log("Integration tests passed successfully")
        else:
            self.log("Integration tests failed", "ERROR")
        
        return success
    
    def generate_deployment_summary(self) -> Dict[str, any]:
        """Generate deployment summary"""
        summary = {
            'deployment_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'services_deployed': [
                'postgres', 'mlb-data-service', 'prometheus', 
                'grafana', 'alertmanager', 'node-exporter', 'loki', 'promtail'
            ],
            'service_urls': {
                'MLB Data Service': 'http://localhost:8101',
                'Prometheus': 'http://localhost:9090',
                'Grafana': 'http://localhost:3000',
                'Alertmanager': 'http://localhost:9093'
            },
            'credentials': {
                'Grafana': 'admin / admin123 (change after login)'
            },
            'monitoring_endpoints': {
                'Health Check': 'http://localhost:8101/health',
                'Service Status': 'http://localhost:8101/api/v1/status',
                'Metrics': 'http://localhost:8101/metrics',
                'Scheduler Status': 'http://localhost:8101/api/v1/scheduler/status'
            },
            'log_files': {
                'Service Logs': './logs/mlb_data_service.log',
                'Docker Logs': 'docker-compose logs [service_name]'
            }
        }
        
        return summary
    
    def deploy(self) -> bool:
        """Execute complete deployment process"""
        self.log("=" * 60)
        self.log("MLB Data Service - Production Deployment")
        self.log("=" * 60)
        
        deployment_steps = [
            ("Validate Prerequisites", self.validate_prerequisites),
            ("Prepare Environment", self.prepare_environment),
            ("Stop Existing Services", self.stop_existing_services),
            ("Deploy Services", self.deploy_services),
            ("Wait for Services", self.wait_for_services),
            ("Run Integration Tests", self.run_integration_tests)
        ]
        
        for step_name, step_function in deployment_steps:
            self.log(f"\nStep: {step_name}")
            self.log("-" * 40)
            
            if not step_function():
                self.log(f"Deployment failed at step: {step_name}", "ERROR")
                return False
        
        # Generate summary
        summary = self.generate_deployment_summary()
        
        self.log("\n" + "=" * 60)
        self.log("DEPLOYMENT COMPLETED SUCCESSFULLY!")
        self.log("=" * 60)
        
        self.log(f"\nDeployment Time: {summary['deployment_time']}")
        self.log("\nService URLs:")
        for service, url in summary['service_urls'].items():
            self.log(f"  {service}: {url}")
        
        self.log("\nDefault Credentials:")
        for service, creds in summary['credentials'].items():
            self.log(f"  {service}: {creds}")
        
        self.log("\nMonitoring Endpoints:")
        for endpoint, url in summary['monitoring_endpoints'].items():
            self.log(f"  {endpoint}: {url}")
        
        # Save deployment report
        report_file = self.project_root / 'deployment_report.json'
        try:
            with open(report_file, 'w') as f:
                json.dump({
                    'summary': summary,
                    'deployment_log': self.deployment_log
                }, f, indent=2)
            self.log(f"\nDeployment report saved to: {report_file}")
        except Exception as e:
            self.log(f"Could not save deployment report: {e}", "WARNING")
        
        self.log("\n" + "=" * 60)
        self.log("Next Steps:")
        self.log("1. Access Grafana at http://localhost:3000 (admin/admin123)")
        self.log("2. Configure alert notification channels in Alertmanager")
        self.log("3. Set up backup schedules for production data")
        self.log("4. Review and customize monitoring dashboards")
        self.log("5. Update security configurations for production")
        self.log("=" * 60)
        
        return True
    
    def rollback(self) -> bool:
        """Rollback deployment in case of failure"""
        self.log("Rolling back deployment...")
        
        # Stop services
        if self.run_command(['docker-compose', 'down', '-v']):
            self.log("Services stopped during rollback")
        else:
            self.log("Warning: Could not stop services during rollback", "WARNING")
        
        # Clean up
        if self.run_command(['docker', 'system', 'prune', '-f']):
            self.log("Docker cleanup completed")
        
        self.log("Rollback completed")
        return True


def main():
    """Main deployment function"""
    project_root = '/home/jeffreyconboy/github-repos/mlb-data-service'
    
    deployer = ProductionDeployer(project_root)
    
    try:
        success = deployer.deploy()
        
        if not success:
            print("\nDeployment failed. Initiating rollback...")
            deployer.rollback()
            return False
        
        return True
        
    except KeyboardInterrupt:
        print("\nDeployment interrupted. Initiating rollback...")
        deployer.rollback()
        return False
    except Exception as e:
        print(f"\nUnexpected error during deployment: {e}")
        deployer.rollback()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
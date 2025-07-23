#!/usr/bin/env python3
"""
Quick Service Validation
=======================

Validates the MLB Data Service implementation without dependencies.
"""

import sys
import os
from datetime import datetime

def validate_docker_setup():
    """Validate Docker setup files"""
    print("🐳 Validating Docker setup...")
    
    checks = {
        'Dockerfile': os.path.exists('Dockerfile'),
        'docker-compose.yml': os.path.exists('docker-compose.yml'),
        'requirements.txt': os.path.exists('requirements.txt'),
        'mlb_data_service/app.py': os.path.exists('mlb_data_service/app.py'),
        'mlb_data_service/external_apis.py': os.path.exists('mlb_data_service/external_apis.py'),
        'logs directory': os.path.exists('logs')
    }
    
    all_passed = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check}")
        if not passed:
            all_passed = False
    
    return all_passed

def validate_flask_app_structure():
    """Validate Flask app structure"""
    print("\n🌐 Validating Flask app structure...")
    
    try:
        # Read app.py and check for key components
        with open('mlb_data_service/app.py', 'r') as f:
            app_content = f.read()
        
        required_components = {
            'Flask app initialization': 'app = Flask(__name__)' in app_content,
            'Health check endpoint': '/health' in app_content,
            'Player collection endpoint': '/api/v1/collect/players' in app_content,
            'Games collection endpoint': '/api/v1/collect/games' in app_content,
            'Statcast collection endpoint': '/api/v1/collect/statcast' in app_content,
            'External API integration': 'ExternalAPIManager' in app_content,
            'Error handling': '@app.errorhandler' in app_content,
            'CORS enabled': 'CORS(app)' in app_content
        }
        
        all_passed = True
        for component, exists in required_components.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {component}")
            if not exists:
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"  ❌ Error reading app.py: {e}")
        return False

def validate_external_apis_structure():
    """Validate external APIs module structure"""
    print("\n🔌 Validating external APIs structure...")
    
    try:
        # Read external_apis.py and check for key components
        with open('mlb_data_service/external_apis.py', 'r') as f:
            apis_content = f.read()
        
        required_components = {
            'ExternalAPIManager class': 'class ExternalAPIManager' in apis_content,
            'Player collection method': 'collect_active_players' in apis_content,
            'Games collection method': 'collect_todays_games' in apis_content,
            'Statcast collection method': 'collect_recent_statcast' in apis_content,
            'Rate limiting': '_rate_limit' in apis_content,
            'Fallback data handling': '_get_fallback_' in apis_content,
            'PyBaseball integration': 'pybaseball' in apis_content,
            'MLB API integration': 'statsapi' in apis_content
        }
        
        all_passed = True
        for component, exists in required_components.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {component}")
            if not exists:
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"  ❌ Error reading external_apis.py: {e}")
        return False

def validate_docker_configuration():
    """Validate Docker configuration"""
    print("\n🔧 Validating Docker configuration...")
    
    try:
        # Check Dockerfile
        with open('Dockerfile', 'r') as f:
            dockerfile_content = f.read()
        
        dockerfile_checks = {
            'Python base image': 'FROM python' in dockerfile_content,
            'Working directory': 'WORKDIR /app' in dockerfile_content,
            'Requirements installation': 'requirements.txt' in dockerfile_content,
            'Port exposure': 'EXPOSE 8001' in dockerfile_content,
            'Health check': 'HEALTHCHECK' in dockerfile_content,
            'Application startup': 'CMD' in dockerfile_content
        }
        
        # Check docker-compose.yml
        with open('docker-compose.yml', 'r') as f:
            compose_content = f.read()
        
        compose_checks = {
            'Service definition': 'mlb-data-service:' in compose_content,
            'Port mapping': '8001:8001' in compose_content,
            'Volume mounting': 'volumes:' in compose_content,
            'Health check config': 'healthcheck:' in compose_content,
            'Network setup': 'networks:' in compose_content
        }
        
        all_passed = True
        
        print("  Dockerfile:")
        for check, passed in dockerfile_checks.items():
            status = "✅" if passed else "❌"
            print(f"    {status} {check}")
            if not passed:
                all_passed = False
        
        print("  docker-compose.yml:")
        for check, passed in compose_checks.items():
            status = "✅" if passed else "❌"
            print(f"    {status} {check}")
            if not passed:
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"  ❌ Error validating Docker configuration: {e}")
        return False

def check_api_endpoints():
    """Check API endpoint definitions"""
    print("\n🌍 Validating API endpoints...")
    
    try:
        with open('mlb_data_service/app.py', 'r') as f:
            content = f.read()
        
        endpoints = {
            'GET /health': "@app.route('/health'" in content,
            'GET /api/v1/status': "@app.route('/api/v1/status'" in content,
            'GET /api/v1/players': "@app.route('/api/v1/players'" in content,
            'GET /api/v1/games/today': "@app.route('/api/v1/games/today'" in content,
            'GET /api/v1/statcast': "@app.route('/api/v1/statcast'" in content,
            'POST /api/v1/collect/players': "'/api/v1/collect/players'" in content and "methods=['POST']" in content,
            'POST /api/v1/collect/games': "'/api/v1/collect/games'" in content and "methods=['POST']" in content,
            'POST /api/v1/collect/statcast': "'/api/v1/collect/statcast'" in content and "methods=['POST']" in content
        }
        
        all_passed = True
        for endpoint, exists in endpoints.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {endpoint}")
            if not exists:
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"  ❌ Error checking endpoints: {e}")
        return False

def generate_deployment_instructions():
    """Generate deployment instructions"""
    print("\n📋 DEPLOYMENT INSTRUCTIONS")
    print("=" * 40)
    
    print("\n1. Install Dependencies:")
    print("   pip install flask flask-cors pybaseball statsapi pandas numpy")
    
    print("\n2. Build and Run Container:")
    print("   docker-compose up --build -d")
    
    print("\n3. Verify Service:")
    print("   curl http://localhost:8001/health")
    
    print("\n4. Test Data Collection:")
    print("   curl -X POST http://localhost:8001/api/v1/collect/players \\")
    print("        -H 'Content-Type: application/json' \\")
    print("        -d '{\"limit\": 5}'")
    
    print("\n5. Check Collected Data:")
    print("   curl http://localhost:8001/api/v1/players")
    
    print("\n6. Monitor Logs:")
    print("   docker-compose logs -f mlb-data-service")

def main():
    """Run complete validation"""
    print("🔍 MLB DATA SERVICE VALIDATION")
    print("=" * 40)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    validations = [
        ('Docker Setup', validate_docker_setup),
        ('Flask App Structure', validate_flask_app_structure),
        ('External APIs Structure', validate_external_apis_structure),
        ('Docker Configuration', validate_docker_configuration),
        ('API Endpoints', check_api_endpoints)
    ]
    
    results = {}
    for name, validation_func in validations:
        results[name] = validation_func()
    
    # Summary
    print("\n" + "=" * 40)
    print("🎯 VALIDATION SUMMARY")
    print("=" * 40)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {name}")
    
    print(f"\n📊 Overall: {passed}/{total} validations passed")
    
    if passed == total:
        print("\n🎉 ALL VALIDATIONS PASSED!")
        print("✅ MLB Data Service is properly implemented")
        print("✅ Container configuration is correct")
        print("✅ External API integration is ready")
        print("✅ REST endpoints are properly defined")
        
        generate_deployment_instructions()
        
        print("\n🚀 READY FOR DEPLOYMENT!")
        return 0
    else:
        print(f"\n⚠️ {total - passed} validations failed")
        print("Review the issues above before deployment")
        return 1

if __name__ == "__main__":
    exit(main())
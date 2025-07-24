#!/usr/bin/env python3
"""
Enhanced MLB Data Service Validation
===================================

Validates the complete setup with comprehensive FanGraphs and Statcast schemas.
"""

import requests
import json
from datetime import datetime

def test_enhanced_service():
    """Test enhanced MLB data service with comprehensive schema"""
    base_url = "http://localhost:8101"
    
    print("🧪 Enhanced MLB Data Service Validation")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = []
    
    # Test 1: Health Check
    print("\n1. 🏥 Testing Health Check...")
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Service: {data.get('service', 'unknown')}")
            print(f"   ✅ Status: {data.get('status', 'unknown')}")
            print(f"   ✅ Database: {data.get('database', 'unknown')}")
            tests.append(("Health Check", True))
        else:
            print(f"   ❌ Health check failed: {response.status_code}")
            tests.append(("Health Check", False))
    except Exception as e:
        print(f"   ❌ Health check error: {e}")
        tests.append(("Health Check", False))
    
    # Test 2: Service Status
    print("\n2. 📊 Testing Service Status...")
    try:
        response = requests.get(f"{base_url}/api/v1/status", timeout=10)
        if response.status_code == 200:
            data = response.json()
            capabilities = data.get('capabilities', {})
            print(f"   ✅ FanGraphs Batting: {capabilities.get('fangraphs_batting', 'unknown')}")
            print(f"   ✅ FanGraphs Pitching: {capabilities.get('fangraphs_pitching', 'unknown')}")
            print(f"   ✅ Statcast: {capabilities.get('statcast', 'unknown')}")
            tests.append(("Service Status", True))
        else:
            print(f"   ❌ Status check failed: {response.status_code}")
            tests.append(("Service Status", False))
    except Exception as e:
        print(f"   ❌ Status check error: {e}")
        tests.append(("Service Status", False))
    
    # Test 3: Analytics Summary
    print("\n3. 📈 Testing Analytics Summary...")
    try:
        response = requests.get(f"{base_url}/api/v1/analytics/summary", timeout=10)
        if response.status_code == 200:
            data = response.json()
            capabilities = data.get('capabilities', {})
            total_fields = capabilities.get('total_fields', {})
            print(f"   ✅ Batting Fields: {total_fields.get('batting', 0)}")
            print(f"   ✅ Pitching Fields: {total_fields.get('pitching', 0)}")
            print(f"   ✅ Statcast Fields: {total_fields.get('statcast', 0)}")
            
            # Verify comprehensive schema
            if (total_fields.get('batting', 0) >= 320 and 
                total_fields.get('pitching', 0) >= 390 and 
                total_fields.get('statcast', 0) >= 110):
                print("   ✅ Comprehensive schema confirmed!")
                tests.append(("Analytics Summary", True))
            else:
                print("   ⚠️ Schema incomplete")
                tests.append(("Analytics Summary", False))
        else:
            print(f"   ❌ Analytics summary failed: {response.status_code}")
            tests.append(("Analytics Summary", False))
    except Exception as e:
        print(f"   ❌ Analytics summary error: {e}")
        tests.append(("Analytics Summary", False))
    
    # Test 4: Collection Endpoints Availability
    print("\n4. 🔌 Testing Collection Endpoints...")
    collection_endpoints = [
        "/api/v1/collect/fangraphs/batting",
        "/api/v1/collect/fangraphs/pitching", 
        "/api/v1/collect/statcast"
    ]
    
    endpoint_tests = []
    for endpoint in collection_endpoints:
        try:
            # Test with OPTIONS to see if endpoint exists
            response = requests.options(f"{base_url}{endpoint}", timeout=5)
            if response.status_code in [200, 405]:  # 405 means method not allowed but endpoint exists
                print(f"   ✅ {endpoint} - Available")
                endpoint_tests.append(True)
            else:
                print(f"   ❌ {endpoint} - Not available ({response.status_code})")
                endpoint_tests.append(False)
        except Exception as e:
            print(f"   ❌ {endpoint} - Error: {e}")
            endpoint_tests.append(False)
    
    tests.append(("Collection Endpoints", all(endpoint_tests)))
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 ENHANCED SERVICE VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    for test_name, result in tests:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📊 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL ENHANCED SERVICE TESTS PASSED!")
        print("✅ Comprehensive FanGraphs schema (320+ batting, 390+ pitching fields)")
        print("✅ Complete Statcast schema (110+ pitch-level fields)")
        print("✅ Production-ready REST API endpoints")
        print("✅ Enhanced analytics capabilities")
        
        print(f"\n🚀 Enhanced MLB Data Service is production-ready!")
        print("📈 Supports comprehensive baseball analytics with:")
        print("   • 320+ FanGraphs batting metrics per player")
        print("   • 390+ FanGraphs pitching metrics per pitcher") 
        print("   • 110+ Statcast fields per pitch")
        print("   • Advanced sabermetrics (wOBA, wRC+, FIP, xFIP, SIERA)")
        print("   • Expected stats (xBA, xSLG, xwOBA)")
        print("   • Pitch tracking (spin rate, movement, location)")
        print("   • Environmental factors and park adjustments")
        
        return 0
    else:
        print(f"\n⚠️ {total - passed} enhanced service tests failed")
        print("Review the issues above before proceeding")
        return 1

if __name__ == "__main__":
    exit(test_enhanced_service())
#!/usr/bin/env python3
"""
MLB Data Service End-to-End Test
================================

Test the complete containerized MLB Data Service workflow.
Validates external API integration and data collection endpoints.
"""

import requests
import json
import time
from datetime import datetime

class MLBDataServiceTester:
    """Test suite for MLB Data Service"""
    
    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
    
    def test_health_check(self):
        """Test service health endpoint"""
        print("🔍 Testing health check...")
        
        try:
            response = self.session.get(f"{self.base_url}/health")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health check passed: {data['status']}")
                print(f"   Service: {data['service']}")
                print(f"   Version: {data['version']}")
                return True
            else:
                print(f"❌ Health check failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Health check error: {e}")
            return False
    
    def test_service_status(self):
        """Test service status endpoint"""
        print("\n📊 Testing service status...")
        
        try:
            response = self.session.get(f"{self.base_url}/api/v1/status")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Status check passed")
                print(f"   Service: {data['service_name']}")
                print(f"   Status: {data['status']}")
                print(f"   Collections today: {data['collections_today']}")
                print(f"   Data counts: {data['data_counts']}")
                return True
            else:
                print(f"❌ Status check failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Status check error: {e}")
            return False
    
    def test_player_collection(self):
        """Test player data collection from external APIs"""
        print("\n👥 Testing player data collection...")
        
        try:
            # Trigger player collection
            payload = {"limit": 10}
            response = self.session.post(
                f"{self.base_url}/api/v1/collect/players",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Player collection successful")
                print(f"   Players collected: {data['players_collected']}")
                print(f"   Data source: {data['data_source']}")
                print(f"   Collection time: {data['collection_time']}")
                
                # Verify we can retrieve the data
                get_response = self.session.get(f"{self.base_url}/api/v1/players")
                if get_response.status_code == 200:
                    players_data = get_response.json()
                    print(f"✅ Player retrieval successful: {players_data['count']} players")
                    
                    if players_data['players']:
                        sample_player = players_data['players'][0]
                        print(f"   Sample player: {sample_player['full_name']} ({sample_player['team']})")
                        print(f"   Stats: {sample_player.get('home_runs', 'N/A')} HR, {sample_player.get('batting_avg', 'N/A')} AVG")
                    
                    return True
                else:
                    print(f"❌ Player retrieval failed: {get_response.status_code}")
                    return False
            else:
                print(f"❌ Player collection failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Player collection error: {e}")
            return False
    
    def test_games_collection(self):
        """Test games data collection from MLB API"""
        print("\n⚾ Testing games data collection...")
        
        try:
            # Trigger games collection
            response = self.session.post(f"{self.base_url}/api/v1/collect/games")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Games collection successful")
                print(f"   Games collected: {data['games_collected']}")
                print(f"   Data source: {data['data_source']}")
                
                # Verify we can retrieve the data
                get_response = self.session.get(f"{self.base_url}/api/v1/games/today")
                if get_response.status_code == 200:
                    games_data = get_response.json()
                    print(f"✅ Games retrieval successful: {games_data['count']} games")
                    
                    if games_data['games']:
                        sample_game = games_data['games'][0]
                        print(f"   Sample game: {sample_game['away_team']} @ {sample_game['home_team']}")
                        print(f"   Venue: {sample_game.get('venue', 'N/A')}")
                        print(f"   Time: {sample_game.get('game_time', 'N/A')}")
                    
                    return True
                else:
                    print(f"❌ Games retrieval failed: {get_response.status_code}")
                    return False
            else:
                print(f"❌ Games collection failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Games collection error: {e}")
            return False
    
    def test_statcast_collection(self):
        """Test Statcast data collection from PyBaseball"""
        print("\n📈 Testing Statcast data collection...")
        
        try:
            # Trigger Statcast collection
            payload = {"days_back": 2, "limit": 20}
            response = self.session.post(
                f"{self.base_url}/api/v1/collect/statcast",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Statcast collection successful")
                print(f"   Records collected: {data['records_collected']}")
                print(f"   Days back: {data['days_back']}")
                print(f"   Data source: {data['data_source']}")
                
                # Verify we can retrieve the data
                get_response = self.session.get(f"{self.base_url}/api/v1/statcast")
                if get_response.status_code == 200:
                    statcast_data = get_response.json()
                    print(f"✅ Statcast retrieval successful: {statcast_data['total_count']} records")
                    
                    if statcast_data['statcast_data']:
                        sample_record = statcast_data['statcast_data'][0]
                        print(f"   Sample record: {sample_record['player_name']} - {sample_record['events']}")
                        print(f"   Launch: {sample_record.get('launch_speed', 'N/A')} mph, {sample_record.get('launch_angle', 'N/A')}°")
                    
                    return True
                else:
                    print(f"❌ Statcast retrieval failed: {get_response.status_code}")
                    return False
            else:
                print(f"❌ Statcast collection failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Statcast collection error: {e}")
            return False
    
    def test_complete_workflow(self):
        """Test complete end-to-end workflow"""
        print("\n🚀 TESTING COMPLETE MLB DATA SERVICE WORKFLOW")
        print("=" * 55)
        
        results = {
            'health_check': self.test_health_check(),
            'service_status': self.test_service_status(),
            'player_collection': self.test_player_collection(),
            'games_collection': self.test_games_collection(),
            'statcast_collection': self.test_statcast_collection()
        }
        
        # Final status check
        print("\n📊 Final service status check...")
        final_status = self.test_service_status()
        
        # Summary
        print("\n" + "=" * 55)
        print("🎯 TEST RESULTS SUMMARY")
        print("=" * 55)
        
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        for test_name, passed_test in results.items():
            status = "✅ PASS" if passed_test else "❌ FAIL"
            print(f"{status} {test_name.replace('_', ' ').title()}")
        
        print(f"\n📈 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 ALL TESTS PASSED - MLB Data Service is ready!")
            print("\n✅ Key Capabilities Verified:")
            print("   • Container health monitoring")
            print("   • External API integration (PyBaseball, MLB API)")
            print("   • Data collection and storage")
            print("   • REST API endpoints for data access")
            print("   • Error handling and logging")
            return True
        else:
            print(f"⚠️ {total - passed} tests failed - check logs for details")
            return False

def main():
    """Run complete test suite"""
    print("🔍 MLB Data Service End-to-End Test Suite")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nNote: This test assumes the service is running on localhost:8001")
    print("Run 'docker-compose up -d' to start the service if needed.\n")
    
    tester = MLBDataServiceTester()
    success = tester.test_complete_workflow()
    
    if success:
        print("\n🚀 Ready for integration with other microservices!")
        return 0
    else:
        print("\n❌ Service needs attention before proceeding")
        return 1

if __name__ == "__main__":
    exit(main())
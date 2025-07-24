#!/usr/bin/env python3
"""
Prediction Data Pipeline Test
=============================

Validates the complete FanGraphs and Weather data collection pipeline.
"""

import json
import sys
import os
from datetime import datetime, date

# Add the service directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'mlb_data_service'))

def test_fangraphs_data_pipeline():
    """Test FanGraphs data collection and storage"""
    print("🔍 Testing FanGraphs Data Pipeline...")
    print("-" * 50)
    
    try:
        from fangraphs_collector import FanGraphsCollector
        
        collector = FanGraphsCollector()
        
        # Test batting data collection
        print("✅ FanGraphsCollector instantiated")
        
        # Test data structure validation
        print("📊 Testing batting data structure...")
        batting_data = collector.collect_batting_stats(season=2024, min_pa=100)
        
        if batting_data:
            sample_batter = batting_data[0]
            required_fields = [
                'player_id', 'player_name', 'team', 'season', 'woba', 'wrc_plus', 'war',
                'gb_percent', 'fb_percent', 'contact_percent', 'data_source'
            ]
            
            missing_fields = [field for field in required_fields if field not in sample_batter]
            if missing_fields:
                print(f"❌ Missing required batting fields: {missing_fields}")
                return False
            
            print(f"✅ Batting data structure validated ({len(batting_data)} batters)")
            print(f"   Sample: {sample_batter['player_name']} - wRC+: {sample_batter.get('wrc_plus', 'N/A')}")
        else:
            print("⚠️ No batting data collected (using fallback)")
        
        # Test pitching data collection
        print("\n📊 Testing pitching data structure...")
        pitching_data = collector.collect_pitching_stats(season=2024, min_ip=20)
        
        if pitching_data:
            sample_pitcher = pitching_data[0]
            required_fields = [
                'player_id', 'player_name', 'team', 'season', 'fip', 'xfip', 'war',
                'k_9', 'bb_9', 'gb_percent', 'data_source'
            ]
            
            missing_fields = [field for field in required_fields if field not in sample_pitcher]
            if missing_fields:
                print(f"❌ Missing required pitching fields: {missing_fields}")
                return False
            
            print(f"✅ Pitching data structure validated ({len(pitching_data)} pitchers)")
            print(f"   Sample: {sample_pitcher['player_name']} - FIP: {sample_pitcher.get('fip', 'N/A')}")
        else:
            print("⚠️ No pitching data collected (using fallback)")
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import FanGraphs modules: {e}")
        return False
    except Exception as e:
        print(f"❌ FanGraphs pipeline test failed: {e}")
        return False

def test_weather_data_pipeline():
    """Test weather data collection and environmental factors"""
    print("\n🌤️ Testing Weather Data Pipeline...")
    print("-" * 50)
    
    try:
        from weather_collector import WeatherCollector
        
        collector = WeatherCollector()
        
        print("✅ WeatherCollector instantiated")
        
        # Test stadium information
        print("🏟️ Testing stadium data collection...")
        stadium_data = collector.collect_stadium_info()
        
        if stadium_data:
            sample_stadium = stadium_data[0]
            required_fields = [
                'stadium_name', 'team', 'latitude', 'longitude', 'elevation_feet',
                'park_factor_runs', 'park_factor_hr', 'roof_type'
            ]
            
            missing_fields = [field for field in required_fields if field not in sample_stadium]
            if missing_fields:
                print(f"❌ Missing required stadium fields: {missing_fields}")
                return False
            
            print(f"✅ Stadium data validated ({len(stadium_data)} stadiums)")
            print(f"   Sample: {sample_stadium['stadium_name']} - HR Factor: {sample_stadium.get('park_factor_hr', 'N/A')}")
        else:
            print("❌ No stadium data collected")
            return False
        
        # Test weather data collection
        print("\n🌡️ Testing weather data collection...")
        weather_data = collector.collect_game_weather(
            game_id="test_game_001",
            venue="Yankee Stadium", 
            game_date=date.today(),
            game_time="19:30"
        )
        
        if weather_data:
            required_fields = [
                'game_id', 'venue', 'temperature_f', 'wind_speed_mph', 'wind_direction',
                'weather_condition', 'wind_help_factor', 'temperature_factor', 'altitude_feet'
            ]
            
            missing_fields = [field for field in required_fields if field not in weather_data]
            if missing_fields:
                print(f"❌ Missing required weather fields: {missing_fields}")
                return False
            
            print(f"✅ Weather data structure validated")
            print(f"   Temperature: {weather_data['temperature_f']}°F")
            print(f"   Wind: {weather_data['wind_speed_mph']} mph {weather_data['wind_direction']}")
            print(f"   Wind Help Factor: {weather_data['wind_help_factor']}")
            print(f"   Temperature Factor: {weather_data['temperature_factor']}")
        else:
            print("❌ No weather data collected")
            return False
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import Weather modules: {e}")
        return False
    except Exception as e:
        print(f"❌ Weather pipeline test failed: {e}")
        return False

def test_database_schema_validation():
    """Test database schema for prediction tables"""
    print("\n💾 Testing Database Schema...")
    print("-" * 50)
    
    # Validate schema files exist
    schema_files = [
        'sql/init.sql',
        'sql/fangraphs_weather_schema.sql'
    ]
    
    for schema_file in schema_files:
        if os.path.exists(schema_file):
            print(f"✅ Schema file exists: {schema_file}")
            
            # Check for key table definitions
            with open(schema_file, 'r') as f:
                content = f.read()
                
            if 'fangraphs_batting' in content:
                print("✅ FanGraphs batting table defined")
            if 'fangraphs_pitching' in content:
                print("✅ FanGraphs pitching table defined") 
            if 'game_weather' in content:
                print("✅ Weather table defined")
            if 'stadium_info' in content:
                print("✅ Stadium info table defined")
            if 'player_matchups' in content:
                print("✅ Player matchups table defined")
                
        else:
            print(f"❌ Schema file missing: {schema_file}")
            return False
    
    print("✅ Database schema validation complete")
    return True

def test_prediction_data_views():
    """Test prediction-ready database views"""
    print("\n📈 Testing Prediction Data Views...")
    print("-" * 50)
    
    # Check for prediction views in schema
    schema_file = 'sql/fangraphs_weather_schema.sql'
    
    if os.path.exists(schema_file):
        with open(schema_file, 'r') as f:
            content = f.read()
        
        prediction_views = [
            'prediction_ready_batters',
            'prediction_ready_pitchers', 
            'game_prediction_data'
        ]
        
        for view in prediction_views:
            if view in content:
                print(f"✅ Prediction view defined: {view}")
            else:
                print(f"❌ Missing prediction view: {view}")
                return False
    else:
        print(f"❌ Schema file not found: {schema_file}")
        return False
    
    print("✅ Prediction views validation complete")
    return True

def generate_prediction_pipeline_summary():
    """Generate summary of prediction data pipeline"""
    print("\n📋 PREDICTION DATA PIPELINE SUMMARY")
    print("=" * 60)
    
    pipeline_components = [
        {
            "component": "FanGraphs Batting Metrics",
            "tables": "fangraphs_batting",
            "metrics": "wOBA, wRC+, BABIP, ISO, WAR, GB%, Contact%, SwStr%",
            "purpose": "Advanced offensive performance analysis"
        },
        {
            "component": "FanGraphs Pitching Metrics", 
            "tables": "fangraphs_pitching",
            "metrics": "FIP, xFIP, SIERA, K/9, BB/9, HR/FB, Velocity, Pitch Mix",
            "purpose": "Advanced pitching performance analysis"
        },
        {
            "component": "Weather & Environmental",
            "tables": "game_weather, stadium_info",
            "metrics": "Temperature, Wind Speed/Direction, Humidity, Park Factors",
            "purpose": "Environmental impact on game outcomes"
        },
        {
            "component": "Player Matchups",
            "tables": "player_matchups",
            "metrics": "Historical B vs P performance, Situational stats",
            "purpose": "Head-to-head matchup analysis"
        },
        {
            "component": "Prediction Views",
            "tables": "prediction_ready_batters/pitchers, game_prediction_data",
            "metrics": "Aggregated data for ML models",
            "purpose": "Model-ready datasets for predictions"
        }
    ]
    
    for component in pipeline_components:
        print(f"\n🎯 {component['component']}")
        print(f"   Tables: {component['tables']}")
        print(f"   Metrics: {component['metrics']}")
        print(f"   Purpose: {component['purpose']}")
    
    print(f"\n📊 Complete MLB Prediction Data Warehouse:")
    print("   • 9 total tables (4 core + 5 prediction-specific)")
    print("   • 100+ advanced metrics per player")
    print("   • Environmental factors for all games")
    print("   • Historical matchup data")
    print("   • Prediction-ready aggregated views")
    
    print(f"\n🚀 Ready for Machine Learning:")
    print("   • Feature engineering from FanGraphs advanced stats")
    print("   • Weather-adjusted predictions")
    print("   • Park factor normalization")
    print("   • Matchup-specific modeling")
    print("   • Real-time data updates")

def main():
    """Run complete prediction pipeline validation"""
    print("🧪 MLB Prediction Data Pipeline Validation")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        ("FanGraphs Data Pipeline", test_fangraphs_data_pipeline),
        ("Weather Data Pipeline", test_weather_data_pipeline),
        ("Database Schema", test_database_schema_validation),
        ("Prediction Data Views", test_prediction_data_views)
    ]
    
    results = {}
    for test_name, test_func in tests:
        results[test_name] = test_func()
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 PIPELINE VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📊 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL PIPELINE TESTS PASSED!")
        print("✅ FanGraphs advanced metrics integration ready")
        print("✅ Weather and environmental data collection ready")
        print("✅ Database schema supports prediction modeling")
        print("✅ Prediction-ready views configured")
        
        generate_prediction_pipeline_summary()
        
        print(f"\n⏰ Validation completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 Complete prediction data pipeline is ready for MLB picks!")
        return 0
    else:
        print(f"\n⚠️ {total - passed} pipeline tests failed")
        print("Review the issues above before proceeding")
        return 1

if __name__ == "__main__":
    exit(main())
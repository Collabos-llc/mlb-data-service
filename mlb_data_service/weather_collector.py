#!/usr/bin/env python3
"""
Weather Data Collector
======================

Collects weather data for MLB games to enhance prediction accuracy.
"""

import logging
import time
import requests
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import math

logger = logging.getLogger(__name__)

class WeatherCollector:
    """Collects weather data for MLB games"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or "demo_api_key"  # Use environment variable in production
        self.base_url = "http://api.openweathermap.org/data/2.5"
        self.rate_limit_delay = 1.0  # seconds between requests
        
        # Stadium coordinates for weather lookup
        self.stadium_locations = {
            "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262, "elevation": 55},
            "Fenway Park": {"lat": 40.3467, "lon": -71.0972, "elevation": 21},
            "Oriole Park at Camden Yards": {"lat": 39.2840, "lon": -76.6214, "elevation": 66},
            "Tropicana Field": {"lat": 27.7682, "lon": -82.6534, "elevation": 15},
            "Rogers Centre": {"lat": 43.6414, "lon": -79.3894, "elevation": 91},
            "Progressive Field": {"lat": 41.4962, "lon": -81.6852, "elevation": 179},
            "Comerica Park": {"lat": 42.3390, "lon": -83.0485, "elevation": 184},
            "Guaranteed Rate Field": {"lat": 41.8300, "lon": -87.6338, "elevation": 179},
            "Kauffman Stadium": {"lat": 39.0517, "lon": -94.4803, "elevation": 229},
            "Target Field": {"lat": 44.9817, "lon": -93.2778, "elevation": 264},
            "Minute Maid Park": {"lat": 29.7570, "lon": -95.3555, "elevation": 22},
            "Angel Stadium": {"lat": 33.8003, "lon": -117.8827, "elevation": 43},
            "Oakland Coliseum": {"lat": 37.7516, "lon": -122.2008, "elevation": 6},
            "T-Mobile Park": {"lat": 47.5914, "lon": -122.3326, "elevation": 134},
            "Globe Life Field": {"lat": 32.7473, "lon": -97.0813, "elevation": 171},
            "Truist Park": {"lat": 33.8906, "lon": -84.4677, "elevation": 320},
            "Marlins Park": {"lat": 25.7781, "lon": -80.2197, "elevation": 8},
            "Citi Field": {"lat": 40.7571, "lon": -73.8458, "elevation": 12},
            "Citizens Bank Park": {"lat": 39.9061, "lon": -75.1665, "elevation": 20},
            "Nationals Park": {"lat": 38.8730, "lon": -77.0074, "elevation": 17},
            "Wrigley Field": {"lat": 41.9484, "lon": -87.6553, "elevation": 180},
            "Great American Ball Park": {"lat": 39.0974, "lon": -84.5061, "elevation": 168},
            "Miller Park": {"lat": 43.0280, "lon": -87.9712, "elevation": 204},
            "PNC Park": {"lat": 40.4469, "lon": -80.0058, "elevation": 228},
            "Busch Stadium": {"lat": 38.6226, "lon": -90.1928, "elevation": 141},
            "Chase Field": {"lat": 33.4453, "lon": -112.0667, "elevation": 331},
            "Coors Field": {"lat": 39.7559, "lon": -104.9942, "elevation": 1580},
            "Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "elevation": 115},
            "Petco Park": {"lat": 32.7073, "lon": -117.1566, "elevation": 62},
            "Oracle Park": {"lat": 37.7786, "lon": -122.3893, "elevation": 12}
        }
        
    def _rate_limit(self):
        """Rate limiting for weather API"""
        time.sleep(self.rate_limit_delay)
    
    def collect_game_weather(self, game_id: str, venue: str, game_date: date, 
                           game_time: str = None) -> Optional[Dict[str, Any]]:
        """Collect weather data for a specific game"""
        logger.info(f"Collecting weather for {venue} on {game_date}")
        
        try:
            # Get stadium coordinates
            if venue not in self.stadium_locations:
                logger.warning(f"Unknown venue: {venue}, using fallback weather")
                return self._get_fallback_weather(game_id, venue, game_date, game_time)
            
            location = self.stadium_locations[venue]
            
            self._rate_limit()
            
            # Determine if we need forecast or historical data
            today = date.today()
            if game_date > today:
                # Future game - get forecast
                weather_data = self._get_weather_forecast(location, game_date, game_time)
            elif game_date == today:
                # Today's game - get current weather
                weather_data = self._get_current_weather(location)
            else:
                # Past game - use historical/fallback data
                weather_data = self._get_fallback_weather(game_id, venue, game_date, game_time)
            
            if weather_data:
                # Add game-specific information
                weather_data['game_id'] = game_id
                weather_data['venue'] = venue
                weather_data['game_date'] = game_date.isoformat()
                weather_data['elevation_feet'] = location['elevation']
                
                # Calculate wind and environmental factors
                weather_data.update(self._calculate_environmental_factors(weather_data, location))
                
                return weather_data
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to collect weather for {venue}: {e}")
            return self._get_fallback_weather(game_id, venue, game_date, game_time)
    
    def _get_current_weather(self, location: Dict[str, float]) -> Optional[Dict[str, Any]]:
        """Get current weather conditions"""
        try:
            url = f"{self.base_url}/weather"
            params = {
                'lat': location['lat'],
                'lon': location['lon'],
                'appid': self.api_key,
                'units': 'imperial'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_weather_response(data, is_forecast=False)
            else:
                logger.warning(f"Weather API returned {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting current weather: {e}")
            return None
    
    def _get_weather_forecast(self, location: Dict[str, float], target_date: date, 
                            game_time: str = None) -> Optional[Dict[str, Any]]:
        """Get weather forecast for future date"""
        try:
            url = f"{self.base_url}/forecast"
            params = {
                'lat': location['lat'],
                'lon': location['lon'],
                'appid': self.api_key,
                'units': 'imperial'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Find forecast closest to game time
                target_datetime = datetime.combine(target_date, datetime.strptime(game_time or "19:00", "%H:%M").time())
                
                best_forecast = None
                min_time_diff = float('inf')
                
                for forecast in data.get('list', []):
                    forecast_dt = datetime.fromtimestamp(forecast['dt'])
                    time_diff = abs((forecast_dt - target_datetime).total_seconds())
                    
                    if time_diff < min_time_diff:
                        min_time_diff = time_diff
                        best_forecast = forecast
                
                if best_forecast:
                    hours_ahead = min_time_diff / 3600
                    weather_data = self._parse_weather_response({'current': best_forecast}, is_forecast=True)
                    weather_data['forecast_hours_ahead'] = int(hours_ahead)
                    return weather_data
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting weather forecast: {e}")
            return None
    
    def _parse_weather_response(self, data: Dict[str, Any], is_forecast: bool = False) -> Dict[str, Any]:
        """Parse weather API response into our format"""
        
        # Handle both current weather and forecast formats
        weather_data = data.get('current', data)
        main = weather_data.get('main', {})
        wind = weather_data.get('wind', {})
        weather = weather_data.get('weather', [{}])[0]
        clouds = weather_data.get('clouds', {})
        
        parsed = {
            'temperature_f': int(main.get('temp', 70)),
            'humidity_percent': int(main.get('humidity', 50)),
            'wind_speed_mph': round(wind.get('speed', 5.0), 1),
            'wind_direction_degrees': wind.get('deg', 180),
            'barometric_pressure': round(main.get('pressure', 30.0) * 0.02953, 2),  # Convert hPa to inHg
            'weather_condition': weather.get('main', 'Clear'),
            'cloud_cover_percent': clouds.get('all', 0),
            'visibility_miles': round(weather_data.get('visibility', 10000) * 0.000621371, 1),  # Convert m to miles
            'precipitation_chance': 0,  # Not available in current API
            'precipitation_amount': 0.0,
            'uv_index': 5,  # Default moderate UV
            'is_forecast': is_forecast,
            'weather_api_source': 'openweathermap'
        }
        
        # Convert wind direction to cardinal direction
        parsed['wind_direction'] = self._degrees_to_cardinal(parsed['wind_direction_degrees'])
        
        # Set dome type based on common knowledge (would be better from stadium_info table)
        parsed['dome_type'] = 'outdoor'  # Most stadiums are outdoor
        
        return parsed
    
    def _degrees_to_cardinal(self, degrees: int) -> str:
        """Convert wind direction degrees to cardinal direction"""
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                     'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        
        # Normalize degrees to 0-360
        degrees = degrees % 360
        
        # Calculate index (16 directions, so 360/16 = 22.5 degrees each)
        index = int((degrees + 11.25) / 22.5) % 16
        
        return directions[index]
    
    def _calculate_environmental_factors(self, weather_data: Dict[str, Any], 
                                       location: Dict[str, float]) -> Dict[str, Any]:
        """Calculate environmental factors that affect baseball"""
        
        temp_f = weather_data.get('temperature_f', 70)
        humidity = weather_data.get('humidity_percent', 50)
        wind_speed = weather_data.get('wind_speed_mph', 0)
        wind_dir_deg = weather_data.get('wind_direction_degrees', 180)
        elevation = location.get('elevation', 0)
        
        # Temperature factor: warmer air = less dense = ball carries farther
        # Baseline at 70°F, +1% distance per 10°F above, -1% per 10°F below
        temp_factor = 1.0 + (temp_f - 70) / 1000
        
        # Humidity factor: humid air = less dense = ball carries farther
        # Baseline at 50% humidity, +0.5% distance per 10% humidity above
        humidity_factor = 1.0 + (humidity - 50) / 2000
        
        # Wind help factor: -1 (strong headwind) to +1 (strong tailwind)
        # Assumes home plate to center field is roughly 0° (north)
        # Wind from south (180°) helps, wind from north (0°) hurts
        wind_help_radians = math.radians(wind_dir_deg)
        wind_component = wind_speed * math.cos(wind_help_radians)  # Positive = tailwind
        wind_help_factor = max(-1.0, min(1.0, wind_component / 20))  # Normalize to -1 to 1
        
        # Altitude factor: higher elevation = thinner air = ball carries farther
        # Roughly 1% more distance per 1000 feet of elevation
        altitude_factor = 1.0 + (elevation / 100000)
        
        return {
            'temperature_factor': round(temp_factor, 3),
            'humidity_factor': round(humidity_factor, 3), 
            'wind_help_factor': round(wind_help_factor, 3),
            'altitude_feet': elevation
        }
    
    def _get_fallback_weather(self, game_id: str, venue: str, game_date: date, 
                            game_time: str = None) -> Dict[str, Any]:
        """Provide fallback weather data when API is unavailable"""
        logger.info(f"Using fallback weather data for {venue}")
        
        # Seasonal defaults based on typical weather patterns
        month = game_date.month
        
        # Temperature varies by season and location
        if venue in ["Tropicana Field", "Marlins Park", "Minute Maid Park"]:
            # Southern stadiums - warmer
            base_temp = [75, 78, 82, 86, 89, 91, 93, 92, 89, 84, 79, 76][month - 1]
        elif venue in ["Coors Field", "Target Field"]:
            # High altitude/northern - cooler
            base_temp = [45, 52, 62, 68, 75, 82, 86, 84, 76, 65, 52, 42][month - 1]
        else:
            # Most stadiums - moderate
            base_temp = [55, 62, 68, 74, 81, 86, 89, 87, 81, 72, 62, 56][month - 1]
        
        fallback_weather = {
            'game_id': game_id,
            'venue': venue,
            'game_date': game_date.isoformat(),
            'game_time': game_time,
            'temperature_f': base_temp,
            'humidity_percent': 55,
            'wind_speed_mph': 8.0,
            'wind_direction': 'SW',
            'wind_direction_degrees': 225,
            'barometric_pressure': 29.92,
            'weather_condition': 'Clear',
            'precipitation_chance': 10,
            'precipitation_amount': 0.0,
            'cloud_cover_percent': 20,
            'visibility_miles': 10.0,
            'uv_index': 6,
            'wind_help_factor': 0.1,
            'temperature_factor': 1.0 + (base_temp - 70) / 1000,
            'humidity_factor': 1.0 + (55 - 50) / 2000,
            'altitude_feet': self.stadium_locations.get(venue, {}).get('elevation', 100),
            'dome_type': 'retractable' if venue in ['Minute Maid Park', 'Chase Field', 'Marlins Park'] else 'outdoor',
            'weather_api_source': 'fallback',
            'forecast_hours_ahead': 0,
            'is_forecast': False,
            'data_source': 'weather_fallback'
        }
        
        return fallback_weather
    
    def collect_stadium_info(self) -> List[Dict[str, Any]]:
        """Collect stadium information for all MLB venues"""
        logger.info("Collecting stadium information data")
        
        stadiums = []
        
        # MLB stadium data with comprehensive information
        stadium_data = [
            {
                'stadium_name': 'Yankee Stadium', 'team': 'NYY', 'city': 'Bronx', 'state': 'NY',
                'latitude': 40.8296, 'longitude': -73.9262, 'elevation_feet': 55, 'timezone': 'America/New_York',
                'capacity': 47309, 'surface_type': 'grass', 'roof_type': 'outdoor',
                'left_field_distance': 318, 'center_field_distance': 408, 'right_field_distance': 314,
                'left_field_height': 8, 'right_field_height': 8, 'foul_territory_factor': 0.95,
                'park_factor_runs': 1.03, 'park_factor_hr': 1.05, 'park_factor_hits': 1.01
            },
            {
                'stadium_name': 'Fenway Park', 'team': 'BOS', 'city': 'Boston', 'state': 'MA',
                'latitude': 40.3467, 'longitude': -71.0972, 'elevation_feet': 21, 'timezone': 'America/New_York',
                'capacity': 37755, 'surface_type': 'grass', 'roof_type': 'outdoor',
                'left_field_distance': 310, 'center_field_distance': 420, 'right_field_distance': 302,
                'left_field_height': 37, 'right_field_height': 3, 'foul_territory_factor': 0.90,
                'park_factor_runs': 1.02, 'park_factor_hr': 0.98, 'park_factor_hits': 1.03
            },
            {
                'stadium_name': 'Coors Field', 'team': 'COL', 'city': 'Denver', 'state': 'CO',
                'latitude': 39.7559, 'longitude': -104.9942, 'elevation_feet': 1580, 'timezone': 'America/Denver',
                'capacity': 50398, 'surface_type': 'grass', 'roof_type': 'outdoor',
                'left_field_distance': 347, 'center_field_distance': 415, 'right_field_distance': 350,
                'left_field_height': 8, 'right_field_height': 8, 'foul_territory_factor': 1.15,
                'park_factor_runs': 1.18, 'park_factor_hr': 1.25, 'park_factor_hits': 1.12
            },
            {
                'stadium_name': 'Minute Maid Park', 'team': 'HOU', 'city': 'Houston', 'state': 'TX',
                'latitude': 29.7570, 'longitude': -95.3555, 'elevation_feet': 22, 'timezone': 'America/Chicago',
                'capacity': 41168, 'surface_type': 'grass', 'roof_type': 'retractable',
                'left_field_distance': 315, 'center_field_distance': 436, 'right_field_distance': 326,
                'left_field_height': 19, 'right_field_height': 7, 'foul_territory_factor': 0.92,
                'park_factor_runs': 0.97, 'park_factor_hr': 0.95, 'park_factor_hits': 0.99
            },
            {
                'stadium_name': 'Dodger Stadium', 'team': 'LAD', 'city': 'Los Angeles', 'state': 'CA',
                'latitude': 34.0739, 'longitude': -118.2400, 'elevation_feet': 115, 'timezone': 'America/Los_Angeles',
                'capacity': 56000, 'surface_type': 'grass', 'roof_type': 'outdoor',
                'left_field_distance': 330, 'center_field_distance': 395, 'right_field_distance': 330,
                'left_field_height': 8, 'right_field_height': 8, 'foul_territory_factor': 1.05,
                'park_factor_runs': 0.96, 'park_factor_hr': 0.93, 'park_factor_hits': 0.97
            }
        ]
        
        for stadium in stadium_data:
            stadium['data_source'] = 'manual_entry'
            stadiums.append(stadium)
        
        logger.info(f"Collected information for {len(stadiums)} stadiums")
        return stadiums
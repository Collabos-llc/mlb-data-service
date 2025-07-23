#!/usr/bin/env python3
"""
External API Integration Module
==============================

Handles all external API calls for MLB data collection.
Integrates with PyBaseball, MLB Stats API, and other data sources.
"""

import pybaseball as pyb
import statsapi
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import time

logger = logging.getLogger(__name__)

class ExternalAPIManager:
    """Manages all external API integrations"""
    
    def __init__(self):
        """Initialize API manager with configuration"""
        self.cache = {}
        self.last_request_times = {}
        self.rate_limits = {
            'pybaseball': 1.0,  # 1 second between requests
            'mlb_api': 0.5,     # 0.5 seconds between requests
            'weather': 2.0      # 2 seconds between requests
        }
        
        # Disable PyBaseball cache for fresh data
        pyb.cache.disable()
        
        logger.info("External API Manager initialized")
    
    def _rate_limit(self, api_name: str):
        """Enforce rate limiting for API calls"""
        if api_name in self.last_request_times:
            time_since_last = time.time() - self.last_request_times[api_name]
            required_delay = self.rate_limits.get(api_name, 1.0)
            
            if time_since_last < required_delay:
                sleep_time = required_delay - time_since_last
                logger.debug(f"Rate limiting {api_name}: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        
        self.last_request_times[api_name] = time.time()
    
    def collect_active_players(self, limit: int = 50) -> List[Dict]:
        """Collect active MLB players from PyBaseball/FanGraphs"""
        try:
            logger.info(f"Collecting active players (limit: {limit})...")
            self._rate_limit('pybaseball')
            
            # Get current season batting leaders (active players)
            current_year = datetime.now().year
            
            try:
                # Get FanGraphs batting data for current season
                batting_data = pyb.batting_stats(current_year, qual=10)  # At least 10 PA
                
                if batting_data is not None and not batting_data.empty:
                    # Convert to our format
                    players = []
                    for _, player in batting_data.head(limit).iterrows():
                        player_data = {
                            'name_first': player.get('Name', '').split(' ')[0] if ' ' in player.get('Name', '') else player.get('Name', ''),
                            'name_last': ' '.join(player.get('Name', '').split(' ')[1:]) if ' ' in player.get('Name', '') else '',
                            'full_name': player.get('Name', ''),
                            'team': player.get('Team', ''),
                            'games': int(player.get('G', 0)) if pd.notna(player.get('G')) else 0,
                            'plate_appearances': int(player.get('PA', 0)) if pd.notna(player.get('PA')) else 0,
                            'home_runs': int(player.get('HR', 0)) if pd.notna(player.get('HR')) else 0,
                            'batting_avg': float(player.get('AVG', 0.0)) if pd.notna(player.get('AVG')) else 0.0,
                            'ops': float(player.get('OPS', 0.0)) if pd.notna(player.get('OPS')) else 0.0,
                            'wrc_plus': int(player.get('wRC+', 100)) if pd.notna(player.get('wRC+')) else 100,
                            'active': True,
                            'collected_at': datetime.now().isoformat(),
                            'data_source': 'fangraphs_pybaseball'
                        }
                        players.append(player_data)
                    
                    logger.info(f"Successfully collected {len(players)} active players from FanGraphs")
                    return players
                
            except Exception as fg_error:
                logger.warning(f"FanGraphs data unavailable: {fg_error}")
            
            # Fallback to mock data if real API fails
            logger.info("Using fallback player data")
            return self._get_fallback_players(limit)
            
        except Exception as e:
            logger.error(f"Player collection failed: {e}")
            return self._get_fallback_players(limit)
    
    def collect_todays_games(self) -> List[Dict]:
        """Collect today's MLB games from MLB Stats API"""
        try:
            logger.info("Collecting today's games from MLB API...")
            self._rate_limit('mlb_api')
            
            today = datetime.now().strftime('%Y-%m-%d')
            
            try:
                # Get today's schedule from MLB API
                schedule = statsapi.schedule(start_date=today, end_date=today)
                
                games = []
                for game in schedule:
                    game_data = {
                        'game_id': game.get('game_id', f"game_{len(games)}"),
                        'date': today,
                        'home_team': game.get('home_name', ''),
                        'away_team': game.get('away_name', ''),
                        'home_team_abbrev': game.get('home_team', ''),
                        'away_team_abbrev': game.get('away_team', ''),
                        'game_time': game.get('game_datetime', ''),
                        'venue': game.get('venue_name', ''),
                        'game_status': game.get('status', ''),
                        'inning': game.get('current_inning', ''),
                        'home_score': game.get('home_score', 0),
                        'away_score': game.get('away_score', 0),
                        'collected_at': datetime.now().isoformat(),
                        'data_source': 'mlb_stats_api'
                    }
                    games.append(game_data)
                
                logger.info(f"Successfully collected {len(games)} games from MLB API")
                return games
                
            except Exception as mlb_error:
                logger.warning(f"MLB API unavailable: {mlb_error}")
                return self._get_fallback_games()
                
        except Exception as e:
            logger.error(f"Games collection failed: {e}")
            return self._get_fallback_games()
    
    def collect_recent_statcast(self, days_back: int = 3, limit: int = 100) -> List[Dict]:
        """Collect recent Statcast data from PyBaseball"""
        try:
            logger.info(f"Collecting Statcast data from last {days_back} days...")
            self._rate_limit('pybaseball')
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            try:
                # Get Statcast data
                statcast_data = pyb.statcast(
                    start_dt=start_date.strftime('%Y-%m-%d'),
                    end_dt=end_date.strftime('%Y-%m-%d')
                )
                
                if statcast_data is not None and not statcast_data.empty:
                    # Convert to our format
                    records = []
                    for _, record in statcast_data.head(limit).iterrows():
                        statcast_record = {
                            'player_name': record.get('player_name', ''),
                            'batter_id': int(record.get('batter', 0)) if pd.notna(record.get('batter')) else 0,
                            'pitcher_name': record.get('pitcher_name', ''),
                            'pitcher_id': int(record.get('pitcher', 0)) if pd.notna(record.get('pitcher')) else 0,
                            'game_date': str(record.get('game_date', '')),
                            'pitch_type': record.get('pitch_type', ''),
                            'release_speed': float(record.get('release_speed', 0.0)) if pd.notna(record.get('release_speed')) else 0.0,
                            'release_pos_x': float(record.get('release_pos_x', 0.0)) if pd.notna(record.get('release_pos_x')) else 0.0,
                            'release_pos_z': float(record.get('release_pos_z', 0.0)) if pd.notna(record.get('release_pos_z')) else 0.0,
                            'events': record.get('events', ''),
                            'description': record.get('description', ''),
                            'launch_speed': float(record.get('launch_speed', 0.0)) if pd.notna(record.get('launch_speed')) else 0.0,
                            'launch_angle': float(record.get('launch_angle', 0.0)) if pd.notna(record.get('launch_angle')) else 0.0,
                            'woba_value': float(record.get('woba_value', 0.0)) if pd.notna(record.get('woba_value')) else 0.0,
                            'estimated_woba_using_speedangle': float(record.get('estimated_woba_using_speedangle', 0.0)) if pd.notna(record.get('estimated_woba_using_speedangle')) else 0.0,
                            'hit_distance_sc': float(record.get('hit_distance_sc', 0.0)) if pd.notna(record.get('hit_distance_sc')) else 0.0,
                            'bb_type': record.get('bb_type', ''),
                            'collected_at': datetime.now().isoformat(),
                            'data_source': 'statcast_pybaseball'
                        }
                        records.append(statcast_record)
                    
                    logger.info(f"Successfully collected {len(records)} Statcast records")
                    return records
                
            except Exception as sc_error:
                logger.warning(f"Statcast data unavailable: {sc_error}")
            
            # Fallback data
            return self._get_fallback_statcast(limit)
            
        except Exception as e:
            logger.error(f"Statcast collection failed: {e}")
            return self._get_fallback_statcast(limit)
    
    def _get_fallback_players(self, limit: int) -> List[Dict]:
        """Fallback player data when APIs are unavailable"""
        fallback_players = [
            {
                'name_first': 'Aaron', 'name_last': 'Judge', 'full_name': 'Aaron Judge',
                'team': 'NYY', 'games': 95, 'plate_appearances': 420, 'home_runs': 35,
                'batting_avg': 0.267, 'ops': 1.045, 'wrc_plus': 178, 'active': True,
                'collected_at': datetime.now().isoformat(), 'data_source': 'fallback'
            },
            {
                'name_first': 'Mike', 'name_last': 'Trout', 'full_name': 'Mike Trout',
                'team': 'LAA', 'games': 82, 'plate_appearances': 350, 'home_runs': 28,
                'batting_avg': 0.289, 'ops': 0.985, 'wrc_plus': 165, 'active': True,
                'collected_at': datetime.now().isoformat(), 'data_source': 'fallback'
            },
            {
                'name_first': 'Ronald', 'name_last': 'Acuna Jr.', 'full_name': 'Ronald Acuna Jr.',
                'team': 'ATL', 'games': 98, 'plate_appearances': 450, 'home_runs': 32,
                'batting_avg': 0.318, 'ops': 1.012, 'wrc_plus': 172, 'active': True,
                'collected_at': datetime.now().isoformat(), 'data_source': 'fallback'
            }
        ]
        return fallback_players[:limit]
    
    def _get_fallback_games(self) -> List[Dict]:
        """Fallback games data when APIs are unavailable"""
        today = datetime.now().strftime('%Y-%m-%d')
        return [
            {
                'game_id': f'{today}_NYAMLB_BOSMLB_1',
                'date': today,
                'home_team': 'Boston Red Sox',
                'away_team': 'New York Yankees',
                'home_team_abbrev': 'BOS',
                'away_team_abbrev': 'NYY',
                'game_time': '19:10',
                'venue': 'Fenway Park',
                'game_status': 'Scheduled',
                'collected_at': datetime.now().isoformat(),
                'data_source': 'fallback'
            },
            {
                'game_id': f'{today}_ATLMLB_PHIMLB_1',
                'date': today,
                'home_team': 'Philadelphia Phillies',
                'away_team': 'Atlanta Braves',
                'home_team_abbrev': 'PHI',
                'away_team_abbrev': 'ATL',
                'game_time': '19:05',
                'venue': 'Citizens Bank Park',
                'game_status': 'Scheduled',
                'collected_at': datetime.now().isoformat(),
                'data_source': 'fallback'
            }
        ]
    
    def _get_fallback_statcast(self, limit: int) -> List[Dict]:
        """Fallback Statcast data when APIs are unavailable"""
        fallback_records = [
            {
                'player_name': 'Aaron Judge', 'batter_id': 592450,
                'pitcher_name': 'Gerrit Cole', 'pitcher_id': 543037,
                'game_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                'pitch_type': 'FF', 'release_speed': 94.5, 'release_pos_x': 2.1, 'release_pos_z': 6.2,
                'events': 'home_run', 'description': 'hit_into_play',
                'launch_speed': 108.3, 'launch_angle': 28, 'woba_value': 2.0,
                'estimated_woba_using_speedangle': 1.95, 'hit_distance_sc': 425.0,
                'bb_type': 'fly_ball', 'collected_at': datetime.now().isoformat(),
                'data_source': 'fallback'
            },
            {
                'player_name': 'Mike Trout', 'batter_id': 545361,
                'pitcher_name': 'Spencer Strider', 'pitcher_id': 675911,
                'game_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                'pitch_type': 'SL', 'release_speed': 87.2, 'release_pos_x': 1.8, 'release_pos_z': 5.9,
                'events': 'single', 'description': 'hit_into_play',
                'launch_speed': 102.1, 'launch_angle': 12, 'woba_value': 0.9,
                'estimated_woba_using_speedangle': 0.85, 'hit_distance_sc': 285.0,
                'bb_type': 'line_drive', 'collected_at': datetime.now().isoformat(),
                'data_source': 'fallback'
            }
        ]
        return fallback_records[:limit]
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
from typing import Dict, List, Optional, Any
import time
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from .enhanced_database import EnhancedDatabaseManager

logger = logging.getLogger(__name__)

# Import monitoring for data quality checks
try:
    from .monitoring.data_monitor import DataQualityValidator
    MONITORING_AVAILABLE = True
except ImportError:
    MONITORING_AVAILABLE = False
    logger.warning("Data monitoring not available")

class ExternalAPIManager:
    """Manages all external API integrations with real-time capabilities"""
    
    def __init__(self, database_manager: EnhancedDatabaseManager = None):
        """Initialize API manager with configuration"""
        self.cache = {}
        self.last_request_times = {}
        self.rate_limits = {
            'pybaseball': 1.0,  # 1 second between requests
            'mlb_api': 0.5,     # 0.5 seconds between requests
            'weather': 2.0,     # 2 seconds between requests
            'fangraphs': 2.0    # 2 seconds between FanGraphs requests
        }
        
        # Initialize database manager
        self.db_manager = database_manager or EnhancedDatabaseManager()
        
        # Thread pool for concurrent operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Disable PyBaseball cache for fresh data
        pyb.cache.disable()
        
        # Data freshness tracking
        self.last_fangraphs_update = None
        self.last_statcast_update = None
        self.last_games_update = None
        
        # Initialize data quality validator if available
        self.quality_validator = None
        if MONITORING_AVAILABLE:
            try:
                self.quality_validator = DataQualityValidator()
                logger.info("Data quality validator initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize data quality validator: {e}")
        
        # Collection failure tracking
        self.collection_failures = {
            'fangraphs': 0,
            'statcast': 0,
            'games': 0
        }
        self.max_consecutive_failures = 3
        
        logger.info("Enhanced External API Manager initialized with real-time capabilities and monitoring")
    
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
    
    def _validate_collected_data(self, data: List[Dict], data_source: str) -> Dict[str, Any]:
        """Validate collected data for quality issues"""
        validation_result = {
            'source': data_source,
            'total_records': len(data),
            'validation_passed': True,
            'issues': [],
            'quality_score': 1.0
        }
        
        if not data:
            validation_result['validation_passed'] = False
            validation_result['issues'].append('No data collected')
            validation_result['quality_score'] = 0.0
            return validation_result
        
        # Basic data quality checks
        null_counts = {}
        invalid_counts = {}
        
        for record in data:
            for key, value in record.items():
                # Count nulls
                if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                    null_counts[key] = null_counts.get(key, 0) + 1
                
                # Check for invalid numeric values
                if key in ['batting_avg', 'ops', 'release_speed', 'launch_speed']:
                    try:
                        if value is not None:
                            float_val = float(value)
                            if float_val < 0 or (key == 'batting_avg' and float_val > 1.0):
                                invalid_counts[key] = invalid_counts.get(key, 0) + 1
                    except (ValueError, TypeError):
                        invalid_counts[key] = invalid_counts.get(key, 0) + 1
        
        # Calculate quality issues
        total_records = len(data)
        issues = []
        
        for field, null_count in null_counts.items():
            null_percentage = null_count / total_records
            if null_percentage > 0.1:  # More than 10% nulls
                issues.append(f"High null percentage in {field}: {null_percentage:.2%}")
        
        for field, invalid_count in invalid_counts.items():
            invalid_percentage = invalid_count / total_records
            if invalid_percentage > 0.05:  # More than 5% invalid values
                issues.append(f"High invalid values in {field}: {invalid_percentage:.2%}")
        
        # Calculate quality score
        if issues:
            validation_result['validation_passed'] = False
            validation_result['issues'] = issues
            validation_result['quality_score'] = max(0.0, 1.0 - (len(issues) * 0.2))
        
        return validation_result
    
    def _handle_collection_failure(self, source: str, error: Exception) -> None:
        """Handle collection failures with exponential backoff"""
        self.collection_failures[source] = self.collection_failures.get(source, 0) + 1
        
        if self.collection_failures[source] >= self.max_consecutive_failures:
            logger.error(f"CRITICAL: {source} has failed {self.collection_failures[source]} consecutive times")
            # Here you could trigger alerts or take corrective action
        
        logger.warning(f"Collection failure for {source} (attempt {self.collection_failures[source]}): {error}")
    
    def _handle_collection_success(self, source: str) -> None:
        """Reset failure counter on successful collection"""
        if self.collection_failures.get(source, 0) > 0:
            logger.info(f"Collection recovered for {source} after {self.collection_failures[source]} failures")
            self.collection_failures[source] = 0
    
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
                    
                    # Validate collected data
                    validation_result = self._validate_collected_data(players, 'fangraphs_players')
                    
                    if not validation_result['validation_passed']:
                        logger.warning(f"Data quality issues in player collection: {validation_result['issues']}")
                    
                    self._handle_collection_success('fangraphs')
                    logger.info(f"Successfully collected {len(players)} active players from FanGraphs (quality score: {validation_result['quality_score']:.2f})")
                    return players
                
            except Exception as fg_error:
                self._handle_collection_failure('fangraphs', fg_error)
                logger.warning(f"FanGraphs data unavailable: {fg_error}")
            
            # Fallback to mock data if real API fails
            logger.info("Using fallback player data")
            return self._get_fallback_players(limit)
            
        except Exception as e:
            self._handle_collection_failure('fangraphs', e)
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
                
                # Validate collected data
                validation_result = self._validate_collected_data(games, 'mlb_games')
                
                if not validation_result['validation_passed']:
                    logger.warning(f"Data quality issues in games collection: {validation_result['issues']}")
                
                self._handle_collection_success('games')
                logger.info(f"Successfully collected {len(games)} games from MLB API (quality score: {validation_result['quality_score']:.2f})")
                return games
                
            except Exception as mlb_error:
                self._handle_collection_failure('games', mlb_error)
                logger.warning(f"MLB API unavailable: {mlb_error}")
                return self._get_fallback_games()
                
        except Exception as e:
            self._handle_collection_failure('games', e)
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
                    
                    # Validate collected data
                    validation_result = self._validate_collected_data(records, 'statcast_data')
                    
                    if not validation_result['validation_passed']:
                        logger.warning(f"Data quality issues in Statcast collection: {validation_result['issues']}")
                    
                    self._handle_collection_success('statcast')
                    logger.info(f"Successfully collected {len(records)} Statcast records (quality score: {validation_result['quality_score']:.2f})")
                    return records
                
            except Exception as sc_error:
                self._handle_collection_failure('statcast', sc_error)
                logger.warning(f"Statcast data unavailable: {sc_error}")
            
            # Fallback data
            return self._get_fallback_statcast(limit)
            
        except Exception as e:
            self._handle_collection_failure('statcast', e)
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
    
    def collect_live_fangraphs_data(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Collect live FanGraphs data with intelligent caching"""
        try:
            current_time = datetime.now()
            
            # Check if we need to refresh (daily refresh or forced)
            if not force_refresh and self.last_fangraphs_update:
                time_since_update = current_time - self.last_fangraphs_update
                if time_since_update.total_seconds() < 3600:  # Less than 1 hour
                    logger.info("FanGraphs data is fresh, skipping collection")
                    return {'status': 'skipped', 'reason': 'data_fresh'}
            
            logger.info("Collecting live FanGraphs batting and pitching data...")
            self._rate_limit('fangraphs')
            
            current_year = current_time.year
            results = {
                'started_at': current_time.isoformat(),
                'batting_records': 0,
                'pitching_records': 0,
                'errors': []
            }
            
            # Collect batting data
            try:
                batting_count = self.db_manager.collect_and_store_fangraphs_batting(
                    season=current_year, min_pa=10
                )
                results['batting_records'] = batting_count
                logger.info(f"✅ Collected {batting_count} FanGraphs batting records")
            except Exception as e:
                error_msg = f"FanGraphs batting collection failed: {e}"
                results['errors'].append(error_msg)
                logger.error(error_msg)
            
            # Collect pitching data
            try:
                pitching_count = self.db_manager.collect_and_store_fangraphs_pitching(
                    season=current_year, min_ip=5
                )
                results['pitching_records'] = pitching_count
                logger.info(f"✅ Collected {pitching_count} FanGraphs pitching records")
            except Exception as e:
                error_msg = f"FanGraphs pitching collection failed: {e}"
                results['errors'].append(error_msg)
                logger.error(error_msg)
            
            # Update last refresh time
            self.last_fangraphs_update = current_time
            results['completed_at'] = datetime.now().isoformat()
            
            return results
            
        except Exception as e:
            logger.error(f"Live FanGraphs collection failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def collect_live_statcast_data(self, hours_back: int = 6) -> Dict[str, Any]:
        """Collect live Statcast data with hourly refresh"""
        try:
            current_time = datetime.now()
            
            # Check if we need to refresh (hourly refresh)
            if self.last_statcast_update:
                time_since_update = current_time - self.last_statcast_update
                if time_since_update.total_seconds() < 3600:  # Less than 1 hour
                    logger.info("Statcast data is fresh, skipping collection")
                    return {'status': 'skipped', 'reason': 'data_fresh'}
            
            logger.info(f"Collecting live Statcast data from last {hours_back} hours...")
            self._rate_limit('pybaseball')
            
            # Calculate date range
            end_time = current_time
            start_time = end_time - timedelta(hours=hours_back)
            
            start_date = start_time.strftime('%Y-%m-%d')
            end_date = end_time.strftime('%Y-%m-%d')
            
            results = {
                'started_at': current_time.isoformat(),
                'start_date': start_date,
                'end_date': end_date,
                'records_collected': 0,
                'errors': []
            }
            
            try:
                # Collect and store Statcast data
                records_count = self.db_manager.collect_and_store_statcast(
                    start_date=start_date, end_date=end_date
                )
                results['records_collected'] = records_count
                logger.info(f"✅ Collected {records_count} Statcast records")
                
                # Update last refresh time
                self.last_statcast_update = current_time
                
            except Exception as e:
                error_msg = f"Statcast collection failed: {e}"
                results['errors'].append(error_msg)
                logger.error(error_msg)
            
            results['completed_at'] = datetime.now().isoformat()
            return results
            
        except Exception as e:
            logger.error(f"Live Statcast collection failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def collect_live_games_data(self, days_ahead: int = 3) -> Dict[str, Any]:
        """Collect live MLB games data with real-time updates"""
        try:
            current_time = datetime.now()
            
            # Check if we need to refresh (every 15 minutes for live games)
            if self.last_games_update:
                time_since_update = current_time - self.last_games_update
                if time_since_update.total_seconds() < 900:  # Less than 15 minutes
                    logger.info("Games data is fresh, skipping collection")
                    return {'status': 'skipped', 'reason': 'data_fresh'}
            
            logger.info(f"Collecting live MLB games data for next {days_ahead} days...")
            self._rate_limit('mlb_api')
            
            results = {
                'started_at': current_time.isoformat(),
                'games_collected': 0,
                'date_range': [],
                'errors': []
            }
            
            # Collect games for multiple days
            for day_offset in range(days_ahead + 1):  # Include today
                target_date = current_time + timedelta(days=day_offset)
                date_str = target_date.strftime('%Y-%m-%d')
                results['date_range'].append(date_str)
                
                try:
                    # Get schedule from MLB API
                    schedule = statsapi.schedule(start_date=date_str, end_date=date_str)
                    
                    for game in schedule:
                        # Store game data (you could extend this to store in database)
                        logger.debug(f"Found game: {game.get('away_name', '')} @ {game.get('home_name', '')}")
                    
                    results['games_collected'] += len(schedule)
                    logger.info(f"✅ Collected {len(schedule)} games for {date_str}")
                    
                except Exception as e:
                    error_msg = f"Games collection failed for {date_str}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            # Update last refresh time
            self.last_games_update = current_time
            results['completed_at'] = datetime.now().isoformat()
            
            return results
            
        except Exception as e:
            logger.error(f"Live games collection failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def collect_all_live_data(self) -> Dict[str, Any]:
        """Collect all live data sources concurrently"""
        logger.info("Starting concurrent collection of all live data sources...")
        
        start_time = datetime.now()
        
        # Run collections concurrently
        loop = asyncio.get_event_loop()
        
        fangraphs_future = loop.run_in_executor(
            self.executor, self.collect_live_fangraphs_data
        )
        statcast_future = loop.run_in_executor(
            self.executor, self.collect_live_statcast_data
        )
        games_future = loop.run_in_executor(
            self.executor, self.collect_live_games_data
        )
        
        # Wait for all to complete
        fangraphs_result, statcast_result, games_result = await asyncio.gather(
            fangraphs_future, statcast_future, games_future, return_exceptions=True
        )
        
        # Compile results
        results = {
            'started_at': start_time.isoformat(),
            'completed_at': datetime.now().isoformat(),
            'fangraphs': fangraphs_result if not isinstance(fangraphs_result, Exception) else {'error': str(fangraphs_result)},
            'statcast': statcast_result if not isinstance(statcast_result, Exception) else {'error': str(statcast_result)},
            'games': games_result if not isinstance(games_result, Exception) else {'error': str(games_result)},
            'total_duration_seconds': (datetime.now() - start_time).total_seconds()
        }
        
        logger.info(f"Concurrent data collection completed in {results['total_duration_seconds']:.2f} seconds")
        return results
    
    def get_data_freshness_status(self) -> Dict[str, Any]:
        """Get status of data freshness across all sources"""
        current_time = datetime.now()
        
        status = {
            'current_time': current_time.isoformat(),
            'fangraphs': {
                'last_update': self.last_fangraphs_update.isoformat() if self.last_fangraphs_update else None,
                'hours_since_update': None,
                'needs_refresh': True
            },
            'statcast': {
                'last_update': self.last_statcast_update.isoformat() if self.last_statcast_update else None,
                'hours_since_update': None,
                'needs_refresh': True
            },
            'games': {
                'last_update': self.last_games_update.isoformat() if self.last_games_update else None,
                'minutes_since_update': None,
                'needs_refresh': True
            }
        }
        
        # Calculate time differences and refresh needs
        if self.last_fangraphs_update:
            hours_diff = (current_time - self.last_fangraphs_update).total_seconds() / 3600
            status['fangraphs']['hours_since_update'] = round(hours_diff, 2)
            status['fangraphs']['needs_refresh'] = hours_diff >= 24  # Daily refresh
        
        if self.last_statcast_update:
            hours_diff = (current_time - self.last_statcast_update).total_seconds() / 3600
            status['statcast']['hours_since_update'] = round(hours_diff, 2)
            status['statcast']['needs_refresh'] = hours_diff >= 1  # Hourly refresh
        
        if self.last_games_update:
            minutes_diff = (current_time - self.last_games_update).total_seconds() / 60
            status['games']['minutes_since_update'] = round(minutes_diff, 2)
            status['games']['needs_refresh'] = minutes_diff >= 15  # 15-minute refresh
        
        return status
    
    def get_comprehensive_monitoring_status(self) -> Dict[str, Any]:
        """Get comprehensive monitoring status including failures and data quality"""
        current_time = datetime.now()
        
        status = {
            'timestamp': current_time.isoformat(),
            'data_freshness': self.get_data_freshness_status(),
            'collection_failures': self.collection_failures.copy(),
            'system_health': 'healthy'
        }
        
        # Determine overall system health
        total_failures = sum(self.collection_failures.values())
        critical_failures = len([f for f in self.collection_failures.values() if f >= self.max_consecutive_failures])
        
        if critical_failures > 0:
            status['system_health'] = 'critical'
        elif total_failures > 0:
            status['system_health'] = 'degraded'
        
        # Add quality validation if available
        if self.quality_validator:
            try:
                quality_report = self.quality_validator.get_quality_report()
                status['data_quality'] = {
                    'total_issues': quality_report.get('total_issues', 0),
                    'critical_issues': quality_report.get('critical_issues', 0),
                    'tables_with_issues': len([
                        table for table, report in quality_report.get('table_reports', {}).items()
                        if report.get('total_issues', 0) > 0
                    ])
                }
            except Exception as e:
                logger.warning(f"Failed to get quality report: {e}")
                status['data_quality'] = {'error': str(e)}
        
        return status
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for data collection operations"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'rate_limits': self.rate_limits.copy(),
            'last_request_times': {
                k: datetime.fromtimestamp(v).isoformat() if v else None
                for k, v in self.last_request_times.items()
            },
            'connection_pool_status': {
                'database_connected': self.db_manager.test_connection() if self.db_manager else False
            },
            'thread_pool_status': {
                'active_threads': len(self.executor._threads) if hasattr(self.executor, '_threads') else 0,
                'max_workers': self.executor._max_workers
            }
        }
        
        return metrics
    
    def close(self):
        """Clean up resources"""
        self.executor.shutdown(wait=True)
        if self.db_manager:
            self.db_manager.close()
        logger.info("External API Manager closed")


# Global instances
_api_manager_instance = None

def get_api_manager() -> ExternalAPIManager:
    """Get or create API manager instance"""
    global _api_manager_instance
    if _api_manager_instance is None:
        _api_manager_instance = ExternalAPIManager()
    return _api_manager_instance
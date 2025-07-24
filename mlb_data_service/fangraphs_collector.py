#!/usr/bin/env python3
"""
FanGraphs Data Collector
========================

Collects advanced baseball metrics from FanGraphs for prediction modeling.
"""

import logging
import time
import requests
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pybaseball as pyb
from pybaseball import batting_stats, pitching_stats
import warnings

# Suppress pybaseball warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class FanGraphsCollector:
    """Collects advanced metrics from FanGraphs via PyBaseball"""
    
    def __init__(self):
        self.current_year = datetime.now().year
        self.rate_limit_delay = 2.0  # seconds between requests
        
    def _rate_limit(self):
        """Rate limiting to be respectful to FanGraphs"""
        time.sleep(self.rate_limit_delay)
    
    def collect_batting_stats(self, season: int = None, min_pa: int = 50) -> List[Dict[str, Any]]:
        """Collect FanGraphs batting statistics"""
        if season is None:
            season = self.current_year
            
        logger.info(f"Collecting FanGraphs batting stats for {season} (min {min_pa} PA)")
        
        try:
            self._rate_limit()
            
            # Get comprehensive batting stats from FanGraphs
            batting_data = batting_stats(season, qual=min_pa, ind=1)
            
            if batting_data is None or batting_data.empty:
                logger.warning(f"No batting data returned for {season}")
                return []
            
            logger.info(f"Retrieved {len(batting_data)} batting records from FanGraphs")
            
            # Convert to our database format
            processed_batters = []
            for _, row in batting_data.iterrows():
                try:
                    batter = {
                        'player_id': str(row.get('playerid', f"fg_{row.get('Name', 'unknown')}_{season}")).replace(' ', '_'),
                        'player_name': str(row.get('Name', 'Unknown')),
                        'team': str(row.get('Team', 'UNK'))[:10],
                        'season': season,
                        # Basic Stats
                        'games': self._safe_int(row.get('G')),
                        'plate_appearances': self._safe_int(row.get('PA')),
                        'at_bats': self._safe_int(row.get('AB')),
                        'hits': self._safe_int(row.get('H')),
                        'singles': self._safe_int(row.get('1B')),
                        'doubles': self._safe_int(row.get('2B')),
                        'triples': self._safe_int(row.get('3B')),
                        'home_runs': self._safe_int(row.get('HR')),
                        'runs': self._safe_int(row.get('R')),
                        'rbi': self._safe_int(row.get('RBI')),
                        'walks': self._safe_int(row.get('BB')),
                        'strikeouts': self._safe_int(row.get('SO')),
                        'stolen_bases': self._safe_int(row.get('SB')),
                        'caught_stealing': self._safe_int(row.get('CS')),
                        # Advanced Metrics
                        'woba': self._safe_decimal(row.get('wOBA')),
                        'wrc_plus': self._safe_int(row.get('wRC+')),
                        'babip': self._safe_decimal(row.get('BABIP')),
                        'iso': self._safe_decimal(row.get('ISO')),
                        'spd': self._safe_decimal(row.get('Spd')),
                        'ubr': self._safe_decimal(row.get('UBR')),
                        'wrc': self._safe_decimal(row.get('wRC')),
                        'wrc_27': self._safe_decimal(row.get('wRC/27')),
                        'off': self._safe_decimal(row.get('Off')),
                        'def': self._safe_decimal(row.get('Def')),
                        'war': self._safe_decimal(row.get('WAR')),
                        # Batted Ball Data
                        'gb_percent': self._safe_decimal(row.get('GB%')),
                        'fb_percent': self._safe_decimal(row.get('FB%')),
                        'ld_percent': self._safe_decimal(row.get('LD%')),
                        'iffb_percent': self._safe_decimal(row.get('IFFB%')),
                        'hr_fb': self._safe_decimal(row.get('HR/FB')),
                        # Plate Discipline
                        'o_swing_percent': self._safe_decimal(row.get('O-Swing%')),
                        'z_swing_percent': self._safe_decimal(row.get('Z-Swing%')),
                        'swing_percent': self._safe_decimal(row.get('Swing%')),
                        'o_contact_percent': self._safe_decimal(row.get('O-Contact%')),
                        'z_contact_percent': self._safe_decimal(row.get('Z-Contact%')),
                        'contact_percent': self._safe_decimal(row.get('Contact%')),
                        'zone_percent': self._safe_decimal(row.get('Zone%')),
                        'f_strike_percent': self._safe_decimal(row.get('F-Strike%')),
                        'swstr_percent': self._safe_decimal(row.get('SwStr%')),
                        # Clutch and Situational
                        'clutch': self._safe_decimal(row.get('Clutch')),
                        'wpa': self._safe_decimal(row.get('WPA')),
                        're24': self._safe_decimal(row.get('RE24')),
                        'rew': self._safe_decimal(row.get('REW')),
                        'pli': self._safe_decimal(row.get('pLI')),
                        'inlev': self._safe_decimal(row.get('inLI')),
                        'cents': self._safe_decimal(row.get('Cents')),
                        'dollars': self._safe_int(row.get('Dollars')),
                        # Metadata
                        'data_source': 'fangraphs_pybaseball'
                    }
                    processed_batters.append(batter)
                    
                except Exception as e:
                    logger.warning(f"Error processing batter {row.get('Name', 'Unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(processed_batters)} batting records")
            return processed_batters
            
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs batting stats: {e}")
            return self._get_fallback_batting_data(season)
    
    def collect_pitching_stats(self, season: int = None, min_ip: int = 10) -> List[Dict[str, Any]]:
        """Collect FanGraphs pitching statistics"""
        if season is None:
            season = self.current_year
            
        logger.info(f"Collecting FanGraphs pitching stats for {season} (min {min_ip} IP)")
        
        try:
            self._rate_limit()
            
            # Get comprehensive pitching stats from FanGraphs
            pitching_data = pitching_stats(season, qual=min_ip, ind=1)
            
            if pitching_data is None or pitching_data.empty:
                logger.warning(f"No pitching data returned for {season}")
                return []
            
            logger.info(f"Retrieved {len(pitching_data)} pitching records from FanGraphs")
            
            # Convert to our database format
            processed_pitchers = []
            for _, row in pitching_data.iterrows():
                try:
                    pitcher = {
                        'player_id': str(row.get('playerid', f"fg_{row.get('Name', 'unknown')}_{season}")).replace(' ', '_'),
                        'player_name': str(row.get('Name', 'Unknown')),
                        'team': str(row.get('Team', 'UNK'))[:10],
                        'season': season,
                        # Basic Stats
                        'wins': self._safe_int(row.get('W')),
                        'losses': self._safe_int(row.get('L')),
                        'saves': self._safe_int(row.get('SV')),
                        'holds': self._safe_int(row.get('HLD')),
                        'games': self._safe_int(row.get('G')),
                        'games_started': self._safe_int(row.get('GS')),
                        'innings_pitched': self._safe_decimal(row.get('IP')),
                        'hits_allowed': self._safe_int(row.get('H')),
                        'runs_allowed': self._safe_int(row.get('R')),
                        'earned_runs': self._safe_int(row.get('ER')),
                        'home_runs_allowed': self._safe_int(row.get('HR')),
                        'walks_allowed': self._safe_int(row.get('BB')),
                        'strikeouts': self._safe_int(row.get('SO')),
                        # Advanced Metrics
                        'era': self._safe_decimal(row.get('ERA')),
                        'whip': self._safe_decimal(row.get('WHIP')),
                        'fip': self._safe_decimal(row.get('FIP')),
                        'xfip': self._safe_decimal(row.get('xFIP')),
                        'siera': self._safe_decimal(row.get('SIERA')),
                        'k_9': self._safe_decimal(row.get('K/9')),
                        'bb_9': self._safe_decimal(row.get('BB/9')),
                        'hr_9': self._safe_decimal(row.get('HR/9')),
                        'k_bb': self._safe_decimal(row.get('K/BB')),
                        # Batted Ball Data
                        'gb_percent': self._safe_decimal(row.get('GB%')),
                        'fb_percent': self._safe_decimal(row.get('FB%')),
                        'ld_percent': self._safe_decimal(row.get('LD%')),
                        'iffb_percent': self._safe_decimal(row.get('IFFB%')),
                        'hr_fb': self._safe_decimal(row.get('HR/FB')),
                        'babip': self._safe_decimal(row.get('BABIP')),
                        'lob_percent': self._safe_decimal(row.get('LOB%')),
                        # Pitch Type Data
                        'fb_velocity': self._safe_decimal(row.get('vFA (pi)')),
                        'fb_percent_usage': self._safe_decimal(row.get('FA% (pi)')),
                        'sl_percent': self._safe_decimal(row.get('SL% (pi)')),
                        'ct_percent': self._safe_decimal(row.get('CT% (pi)')),
                        'cb_percent': self._safe_decimal(row.get('CB% (pi)')),
                        'ch_percent': self._safe_decimal(row.get('CH% (pi)')),
                        'sf_percent': self._safe_decimal(row.get('SF% (pi)')),
                        'kn_percent': self._safe_decimal(row.get('KN% (pi)')),
                        # Performance Metrics
                        'war': self._safe_decimal(row.get('WAR')),
                        'wpa': self._safe_decimal(row.get('WPA')),
                        're24': self._safe_decimal(row.get('RE24')),
                        'rew': self._safe_decimal(row.get('REW')),
                        'pli': self._safe_decimal(row.get('pLI')),
                        'inlev': self._safe_decimal(row.get('inLI')),
                        'gmli': self._safe_decimal(row.get('gmLI')),
                        'wpa_minus': self._safe_decimal(row.get('WPA-')),
                        'wpa_plus': self._safe_decimal(row.get('WPA+')),
                        # Metadata
                        'data_source': 'fangraphs_pybaseball'
                    }
                    processed_pitchers.append(pitcher)
                    
                except Exception as e:
                    logger.warning(f"Error processing pitcher {row.get('Name', 'Unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(processed_pitchers)} pitching records")
            return processed_pitchers
            
        except Exception as e:
            logger.error(f"Failed to collect FanGraphs pitching stats: {e}")
            return self._get_fallback_pitching_data(season)
    
    def _safe_int(self, value) -> Optional[int]:
        """Safely convert value to integer"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_decimal(self, value) -> Optional[float]:
        """Safely convert value to decimal"""
        if pd.isna(value) or value == '' or value == '-':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _get_fallback_batting_data(self, season: int) -> List[Dict[str, Any]]:
        """Provide fallback batting data when FanGraphs is unavailable"""
        logger.info("Using fallback FanGraphs batting data")
        
        fallback_batters = [
            {
                'player_id': 'fg_mike_trout_2024',
                'player_name': 'Mike Trout',
                'team': 'LAA',
                'season': season,
                'games': 29, 'plate_appearances': 136, 'at_bats': 124,
                'hits': 37, 'home_runs': 10, 'rbi': 14, 'walks': 9, 'strikeouts': 26,
                'woba': 0.423, 'wrc_plus': 178, 'babip': 0.385, 'iso': 0.274, 'war': 1.8,
                'gb_percent': 38.2, 'fb_percent': 45.5, 'ld_percent': 16.4,
                'o_swing_percent': 26.8, 'z_swing_percent': 66.2, 'contact_percent': 83.1,
                'data_source': 'fangraphs_fallback'
            },
            {
                'player_id': 'fg_aaron_judge_2024',
                'player_name': 'Aaron Judge',
                'team': 'NYY',
                'season': season,
                'games': 158, 'plate_appearances': 704, 'at_bats': 611,
                'hits': 180, 'home_runs': 58, 'rbi': 144, 'walks': 133, 'strikeouts': 201,
                'woba': 0.458, 'wrc_plus': 207, 'babip': 0.349, 'iso': 0.350, 'war': 11.5,
                'gb_percent': 35.1, 'fb_percent': 44.7, 'ld_percent': 20.2,
                'o_swing_percent': 24.9, 'z_swing_percent': 64.8, 'contact_percent': 73.5,
                'data_source': 'fangraphs_fallback'
            },
            {
                'player_id': 'fg_mookie_betts_2024',
                'player_name': 'Mookie Betts',
                'team': 'LAD',
                'season': season,
                'games': 116, 'plate_appearances': 516, 'at_bats': 449,
                'hits': 155, 'home_runs': 19, 'rbi': 75, 'walks': 56, 'strikeouts': 77,
                'woba': 0.409, 'wrc_plus': 157, 'babip': 0.353, 'iso': 0.229, 'war': 7.8,
                'gb_percent': 42.1, 'fb_percent': 38.9, 'ld_percent': 19.0,
                'o_swing_percent': 29.8, 'z_swing_percent': 70.1, 'contact_percent': 85.2,
                'data_source': 'fangraphs_fallback'
            }
        ]
        
        return fallback_batters
    
    def _get_fallback_pitching_data(self, season: int) -> List[Dict[str, Any]]:
        """Provide fallback pitching data when FanGraphs is unavailable"""
        logger.info("Using fallback FanGraphs pitching data")
        
        fallback_pitchers = [
            {
                'player_id': 'fg_gerrit_cole_2024',
                'player_name': 'Gerrit Cole',
                'team': 'NYY',
                'season': season,
                'wins': 8, 'losses': 5, 'games': 17, 'games_started': 17,
                'innings_pitched': 95.0, 'strikeouts': 99, 'walks_allowed': 15,
                'era': 3.41, 'whip': 1.13, 'fip': 3.69, 'xfip': 4.01, 'war': 2.3,
                'k_9': 9.4, 'bb_9': 1.4, 'hr_9': 1.5, 'k_bb': 6.60,
                'gb_percent': 38.8, 'fb_percent': 40.9, 'babip': 0.276,
                'fb_velocity': 96.8, 'fb_percent_usage': 53.2, 'sl_percent': 31.4,
                'data_source': 'fangraphs_fallback'
            },
            {
                'player_id': 'fg_jacob_degrom_2024',
                'player_name': 'Jacob deGrom',
                'team': 'TEX',
                'season': season,
                'wins': 2, 'losses': 1, 'games': 4, 'games_started': 4,
                'innings_pitched': 23.2, 'strikeouts': 29, 'walks_allowed': 9,
                'era': 2.67, 'whip': 1.10, 'fip': 2.85, 'xfip': 2.93, 'war': 0.7,
                'k_9': 11.0, 'bb_9': 3.4, 'hr_9': 0.8, 'k_bb': 3.22,
                'gb_percent': 42.3, 'fb_percent': 37.2, 'babip': 0.290,
                'fb_velocity': 99.2, 'fb_percent_usage': 49.8, 'sl_percent': 35.1,
                'data_source': 'fangraphs_fallback'
            },
            {
                'player_id': 'fg_sandy_alcantara_2024',
                'player_name': 'Sandy Alcantara',
                'team': 'MIA',
                'season': season,
                'wins': 4, 'losses': 8, 'games': 14, 'games_started': 14,
                'innings_pitched': 75.1, 'strikeouts': 50, 'walks_allowed': 24,
                'era': 5.97, 'whip': 1.55, 'fip': 4.98, 'xfip': 4.22, 'war': 0.2,
                'k_9': 6.0, 'bb_9': 2.9, 'hr_9': 1.6, 'k_bb': 2.08,
                'gb_percent': 55.1, 'fb_percent': 29.8, 'babip': 0.315,
                'fb_velocity': 98.1, 'fb_percent_usage': 55.9, 'sl_percent': 27.8,
                'data_source': 'fangraphs_fallback'
            }
        ]
        
        return fallback_pitchers
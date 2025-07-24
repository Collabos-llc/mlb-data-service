#!/usr/bin/env python3
"""
MLB Data Service - Database Manager
===================================

Handles PostgreSQL database connections and operations for MLB data storage.
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import json

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 
            'postgresql://mlb_user:mlb_secure_pass_2024@localhost:5432/mlb_data')
        self.pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=self.database_url
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self.pool:
            return self.pool.getconn()
        return None
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool and conn:
            self.pool.putconn(conn)
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                self.return_connection(conn)
                return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def store_players(self, players_data: List[Dict[str, Any]]) -> int:
        """Store players data in database"""
        if not players_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Clear existing players for fresh data
            cursor.execute("DELETE FROM players")
            
            insert_sql = """
            INSERT INTO players (
                player_id, full_name, team, position, batting_avg, 
                home_runs, rbi, ops, war, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                team = EXCLUDED.team,
                position = EXCLUDED.position,
                batting_avg = EXCLUDED.batting_avg,
                home_runs = EXCLUDED.home_runs,
                rbi = EXCLUDED.rbi,
                ops = EXCLUDED.ops,
                war = EXCLUDED.war,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for player in players_data:
                try:
                    cursor.execute(insert_sql, (
                        player.get('player_id', f"player_{inserted_count}"),
                        player.get('full_name', 'Unknown'),
                        player.get('team', 'UNK'),
                        player.get('position', 'UNK'),
                        player.get('batting_avg'),
                        player.get('home_runs'),
                        player.get('rbi'),
                        player.get('ops'),
                        player.get('war'),
                        player.get('data_source', 'unknown')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert player {player.get('full_name', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} players in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store players: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_players(self, limit: int = None) -> List[Dict[str, Any]]:
        """Retrieve players from database"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            sql = "SELECT * FROM active_players"
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql)
            players = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            # Convert decimal and date types for JSON serialization
            for player in players:
                for key, value in player.items():
                    if hasattr(value, 'isoformat'):
                        player[key] = value.isoformat()
                    elif str(type(value)) == "<class 'decimal.Decimal'>":
                        player[key] = float(value) if value is not None else None
            
            return players
            
        except Exception as e:
            logger.error(f"Failed to retrieve players: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)
    
    def store_games(self, games_data: List[Dict[str, Any]]) -> int:
        """Store games data in database"""
        if not games_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Clear today's games for fresh data
            cursor.execute("DELETE FROM games WHERE game_date = CURRENT_DATE")
            
            insert_sql = """
            INSERT INTO games (
                game_id, game_date, home_team, away_team, home_score, 
                away_score, game_status, venue, game_time, inning, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                game_status = EXCLUDED.game_status,
                inning = EXCLUDED.inning,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for game in games_data:
                try:
                    game_date = game.get('game_date')
                    if isinstance(game_date, str):
                        game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
                    elif not isinstance(game_date, date):
                        game_date = date.today()
                    
                    cursor.execute(insert_sql, (
                        game.get('game_id', f"game_{inserted_count}"),
                        game_date,
                        game.get('home_team', 'UNK'),
                        game.get('away_team', 'UNK'),
                        game.get('home_score'),
                        game.get('away_score'),
                        game.get('game_status', 'scheduled'),
                        game.get('venue'),
                        game.get('game_time'),
                        game.get('inning'),
                        game.get('data_source', 'unknown')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert game {game.get('game_id', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} games in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store games: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_todays_games(self) -> List[Dict[str, Any]]:
        """Retrieve today's games from database"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("SELECT * FROM todays_games")
            games = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            # Convert date and datetime types for JSON serialization
            for game in games:
                for key, value in game.items():
                    if hasattr(value, 'isoformat'):
                        game[key] = value.isoformat()
            
            return games
            
        except Exception as e:
            logger.error(f"Failed to retrieve today's games: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)
    
    def store_statcast(self, statcast_data: List[Dict[str, Any]]) -> int:
        """Store Statcast data in database"""
        if not statcast_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO statcast (
                game_id, player_name, player_id, events, description,
                launch_speed, launch_angle, hit_distance_sc, exit_velocity,
                pitch_type, release_speed, game_date, at_bat_number,
                pitch_number, data_source
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            inserted_count = 0
            for record in statcast_data:
                try:
                    game_date = record.get('game_date')
                    if isinstance(game_date, str):
                        game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
                    elif not isinstance(game_date, date):
                        game_date = date.today()
                    
                    cursor.execute(insert_sql, (
                        record.get('game_id'),
                        record.get('player_name'),
                        record.get('player_id'),
                        record.get('events'),
                        record.get('description'),
                        record.get('launch_speed'),
                        record.get('launch_angle'),
                        record.get('hit_distance_sc'),
                        record.get('exit_velocity'),
                        record.get('pitch_type'),
                        record.get('release_speed'),
                        game_date,
                        record.get('at_bat_number'),
                        record.get('pitch_number'),
                        record.get('data_source', 'unknown')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert Statcast record: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} Statcast records in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store Statcast data: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_statcast_data(self, player_name: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve Statcast data from database"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            sql = "SELECT * FROM statcast"
            params = []
            
            if player_name:
                sql += " WHERE LOWER(player_name) = LOWER(%s)"
                params.append(player_name)
            
            sql += " ORDER BY game_date DESC, at_bat_number, pitch_number"
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql, params)
            statcast = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            # Convert date and decimal types for JSON serialization
            for record in statcast:
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):
                        record[key] = value.isoformat()
                    elif str(type(value)) == "<class 'decimal.Decimal'>":
                        record[key] = float(value) if value is not None else None
            
            return statcast
            
        except Exception as e:
            logger.error(f"Failed to retrieve Statcast data: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)
    
    def log_collection_status(self, collection_type: str, status: str, 
                            records_collected: int = 0, error_message: str = None,
                            data_source: str = None):
        """Log collection job status"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if status == 'running':
                sql = """
                INSERT INTO collection_status (collection_type, status, started_at, data_source)
                VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
                """
                cursor.execute(sql, (collection_type, status, data_source))
            else:
                sql = """
                UPDATE collection_status 
                SET status = %s, completed_at = CURRENT_TIMESTAMP, 
                    records_collected = %s, error_message = %s
                WHERE collection_type = %s AND status = 'running'
                """
                cursor.execute(sql, (status, records_collected, error_message, collection_type))
            
            conn.commit()
            cursor.close()
            
        except Exception as e:
            logger.error(f"Failed to log collection status: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get data counts
            cursor.execute("SELECT COUNT(*) as players_count FROM players")
            players_count = cursor.fetchone()['players_count']
            
            cursor.execute("SELECT COUNT(*) as games_count FROM games WHERE game_date = CURRENT_DATE")
            games_count = cursor.fetchone()['games_count']
            
            cursor.execute("SELECT COUNT(*) as statcast_count FROM statcast WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'")
            statcast_count = cursor.fetchone()['statcast_count']
            
            # Get latest collection times
            cursor.execute("""
                SELECT collection_type, MAX(completed_at) as last_completed
                FROM collection_status 
                WHERE status = 'completed'
                GROUP BY collection_type
            """)
            
            collection_times = {row['collection_type']: row['last_completed'].isoformat() if row['last_completed'] else None 
                              for row in cursor.fetchall()}
            
            cursor.close()
            
            return {
                'data_counts': {
                    'players': players_count,
                    'games_today': games_count,
                    'statcast_recent': statcast_count
                },
                'last_collections': collection_times
            }
            
        except Exception as e:
            logger.error(f"Failed to get collection stats: {e}")
            return {'data_counts': {}, 'last_collections': {}}
        finally:
            if conn:
                self.return_connection(conn)

    def store_fangraphs_batting(self, batting_data: List[Dict[str, Any]]) -> int:
        """Store FanGraphs batting data in database"""
        if not batting_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO fangraphs_batting (
                player_id, player_name, team, season, games, plate_appearances, at_bats,
                hits, singles, doubles, triples, home_runs, runs, rbi, walks, strikeouts,
                stolen_bases, caught_stealing, woba, wrc_plus, babip, iso, spd, ubr, wrc,
                wrc_27, off, def, war, gb_percent, fb_percent, ld_percent, iffb_percent,
                hr_fb, o_swing_percent, z_swing_percent, swing_percent, o_contact_percent,
                z_contact_percent, contact_percent, zone_percent, f_strike_percent,
                swstr_percent, clutch, wpa, re24, rew, pli, inlev, cents, dollars, data_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (player_id, season) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                team = EXCLUDED.team,
                games = EXCLUDED.games,
                plate_appearances = EXCLUDED.plate_appearances,
                woba = EXCLUDED.woba,
                wrc_plus = EXCLUDED.wrc_plus,
                war = EXCLUDED.war,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for batter in batting_data:
                try:
                    cursor.execute(insert_sql, (
                        batter.get('player_id'), batter.get('player_name'), batter.get('team'),
                        batter.get('season'), batter.get('games'), batter.get('plate_appearances'),
                        batter.get('at_bats'), batter.get('hits'), batter.get('singles'),
                        batter.get('doubles'), batter.get('triples'), batter.get('home_runs'),
                        batter.get('runs'), batter.get('rbi'), batter.get('walks'),
                        batter.get('strikeouts'), batter.get('stolen_bases'), batter.get('caught_stealing'),
                        batter.get('woba'), batter.get('wrc_plus'), batter.get('babip'),
                        batter.get('iso'), batter.get('spd'), batter.get('ubr'), batter.get('wrc'),
                        batter.get('wrc_27'), batter.get('off'), batter.get('def'), batter.get('war'),
                        batter.get('gb_percent'), batter.get('fb_percent'), batter.get('ld_percent'),
                        batter.get('iffb_percent'), batter.get('hr_fb'), batter.get('o_swing_percent'),
                        batter.get('z_swing_percent'), batter.get('swing_percent'),
                        batter.get('o_contact_percent'), batter.get('z_contact_percent'),
                        batter.get('contact_percent'), batter.get('zone_percent'),
                        batter.get('f_strike_percent'), batter.get('swstr_percent'),
                        batter.get('clutch'), batter.get('wpa'), batter.get('re24'),
                        batter.get('rew'), batter.get('pli'), batter.get('inlev'),
                        batter.get('cents'), batter.get('dollars'), batter.get('data_source')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert FanGraphs batter {batter.get('player_name', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} FanGraphs batting records in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store FanGraphs batting data: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)
    
    def store_fangraphs_pitching(self, pitching_data: List[Dict[str, Any]]) -> int:
        """Store FanGraphs pitching data in database"""
        if not pitching_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO fangraphs_pitching (
                player_id, player_name, team, season, wins, losses, saves, holds, games,
                games_started, innings_pitched, hits_allowed, runs_allowed, earned_runs,
                home_runs_allowed, walks_allowed, strikeouts, era, whip, fip, xfip, siera,
                k_9, bb_9, hr_9, k_bb, gb_percent, fb_percent, ld_percent, iffb_percent,
                hr_fb, babip, lob_percent, fb_velocity, fb_percent_usage, sl_percent,
                ct_percent, cb_percent, ch_percent, sf_percent, kn_percent, war, wpa,
                re24, rew, pli, inlev, gmli, wpa_minus, wpa_plus, data_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (player_id, season) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                team = EXCLUDED.team,
                games = EXCLUDED.games,
                innings_pitched = EXCLUDED.innings_pitched,
                fip = EXCLUDED.fip,
                xfip = EXCLUDED.xfip,
                war = EXCLUDED.war,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for pitcher in pitching_data:
                try:
                    cursor.execute(insert_sql, (
                        pitcher.get('player_id'), pitcher.get('player_name'), pitcher.get('team'),
                        pitcher.get('season'), pitcher.get('wins'), pitcher.get('losses'),
                        pitcher.get('saves'), pitcher.get('holds'), pitcher.get('games'),
                        pitcher.get('games_started'), pitcher.get('innings_pitched'),
                        pitcher.get('hits_allowed'), pitcher.get('runs_allowed'),
                        pitcher.get('earned_runs'), pitcher.get('home_runs_allowed'),
                        pitcher.get('walks_allowed'), pitcher.get('strikeouts'),
                        pitcher.get('era'), pitcher.get('whip'), pitcher.get('fip'),
                        pitcher.get('xfip'), pitcher.get('siera'), pitcher.get('k_9'),
                        pitcher.get('bb_9'), pitcher.get('hr_9'), pitcher.get('k_bb'),
                        pitcher.get('gb_percent'), pitcher.get('fb_percent'), pitcher.get('ld_percent'),
                        pitcher.get('iffb_percent'), pitcher.get('hr_fb'), pitcher.get('babip'),
                        pitcher.get('lob_percent'), pitcher.get('fb_velocity'),
                        pitcher.get('fb_percent_usage'), pitcher.get('sl_percent'),
                        pitcher.get('ct_percent'), pitcher.get('cb_percent'), pitcher.get('ch_percent'),
                        pitcher.get('sf_percent'), pitcher.get('kn_percent'), pitcher.get('war'),
                        pitcher.get('wpa'), pitcher.get('re24'), pitcher.get('rew'),
                        pitcher.get('pli'), pitcher.get('inlev'), pitcher.get('gmli'),
                        pitcher.get('wpa_minus'), pitcher.get('wpa_plus'), pitcher.get('data_source')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert FanGraphs pitcher {pitcher.get('player_name', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} FanGraphs pitching records in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store FanGraphs pitching data: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)

    def store_game_weather(self, weather_data: List[Dict[str, Any]]) -> int:
        """Store weather data in database"""
        if not weather_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO game_weather (
                game_id, venue, game_date, game_time, temperature_f, humidity_percent,
                wind_speed_mph, wind_direction, wind_direction_degrees, barometric_pressure,
                weather_condition, precipitation_chance, precipitation_amount, cloud_cover_percent,
                visibility_miles, uv_index, wind_help_factor, temperature_factor, humidity_factor,
                altitude_feet, dome_type, weather_api_source, forecast_hours_ahead, is_forecast, data_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (game_id) DO UPDATE SET
                temperature_f = EXCLUDED.temperature_f,
                humidity_percent = EXCLUDED.humidity_percent,
                wind_speed_mph = EXCLUDED.wind_speed_mph,
                wind_direction = EXCLUDED.wind_direction,
                weather_condition = EXCLUDED.weather_condition,
                wind_help_factor = EXCLUDED.wind_help_factor,
                temperature_factor = EXCLUDED.temperature_factor,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for weather in weather_data:
                try:
                    # Parse game_time if it's a string
                    game_time = weather.get('game_time')
                    if isinstance(game_time, str) and game_time:
                        try:
                            game_time = datetime.strptime(game_time, '%H:%M').time()
                        except ValueError:
                            game_time = None
                    
                    # Parse game_date if it's a string
                    game_date = weather.get('game_date')
                    if isinstance(game_date, str):
                        game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
                    
                    cursor.execute(insert_sql, (
                        weather.get('game_id'), weather.get('venue'), game_date, game_time,
                        weather.get('temperature_f'), weather.get('humidity_percent'),
                        weather.get('wind_speed_mph'), weather.get('wind_direction'),
                        weather.get('wind_direction_degrees'), weather.get('barometric_pressure'),
                        weather.get('weather_condition'), weather.get('precipitation_chance'),
                        weather.get('precipitation_amount'), weather.get('cloud_cover_percent'),
                        weather.get('visibility_miles'), weather.get('uv_index'),
                        weather.get('wind_help_factor'), weather.get('temperature_factor'),
                        weather.get('humidity_factor'), weather.get('altitude_feet'),
                        weather.get('dome_type'), weather.get('weather_api_source'),
                        weather.get('forecast_hours_ahead'), weather.get('is_forecast'),
                        weather.get('data_source')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert weather data for game {weather.get('game_id', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} weather records in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store weather data: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)
    
    def store_stadium_info(self, stadium_data: List[Dict[str, Any]]) -> int:
        """Store stadium information in database"""
        if not stadium_data:
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO stadium_info (
                stadium_name, team, city, state, country, latitude, longitude, elevation_feet,
                timezone, capacity, surface_type, roof_type, left_field_distance, center_field_distance,
                right_field_distance, left_field_height, right_field_height, foul_territory_factor,
                park_factor_runs, park_factor_hr, park_factor_hits, park_factor_walks,
                park_factor_so, data_source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (stadium_name) DO UPDATE SET
                team = EXCLUDED.team,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                elevation_feet = EXCLUDED.elevation_feet,
                capacity = EXCLUDED.capacity,
                park_factor_runs = EXCLUDED.park_factor_runs,
                park_factor_hr = EXCLUDED.park_factor_hr,
                data_source = EXCLUDED.data_source,
                updated_at = CURRENT_TIMESTAMP
            """
            
            inserted_count = 0
            for stadium in stadium_data:
                try:
                    cursor.execute(insert_sql, (
                        stadium.get('stadium_name'), stadium.get('team'), stadium.get('city'),
                        stadium.get('state'), stadium.get('country', 'USA'), stadium.get('latitude'),
                        stadium.get('longitude'), stadium.get('elevation_feet'), stadium.get('timezone'),
                        stadium.get('capacity'), stadium.get('surface_type'), stadium.get('roof_type'),
                        stadium.get('left_field_distance'), stadium.get('center_field_distance'),
                        stadium.get('right_field_distance'), stadium.get('left_field_height'),
                        stadium.get('right_field_height'), stadium.get('foul_territory_factor'),
                        stadium.get('park_factor_runs'), stadium.get('park_factor_hr'),
                        stadium.get('park_factor_hits'), stadium.get('park_factor_walks'),
                        stadium.get('park_factor_so'), stadium.get('data_source')
                    ))
                    inserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to insert stadium {stadium.get('stadium_name', 'Unknown')}: {e}")
            
            conn.commit()
            cursor.close()
            logger.info(f"Stored {inserted_count} stadium records in database")
            return inserted_count
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to store stadium data: {e}")
            return 0
        finally:
            if conn:
                self.return_connection(conn)

    def close(self):
        """Close all database connections"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
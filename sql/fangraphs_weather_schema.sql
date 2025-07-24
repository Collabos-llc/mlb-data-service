-- MLB Data Service - FanGraphs and Weather Schema Extension
-- Adds advanced analytics tables for prediction modeling

-- FanGraphs Batting Advanced Metrics
CREATE TABLE IF NOT EXISTS fangraphs_batting (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    games INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    hits INTEGER,
    singles INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    runs INTEGER,
    rbi INTEGER,
    walks INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    -- Advanced Metrics
    woba DECIMAL(5,3),              -- Weighted On-Base Average
    wrc_plus INTEGER,               -- Weighted Runs Created Plus
    babip DECIMAL(5,3),             -- Batting Average on Balls in Play
    iso DECIMAL(5,3),               -- Isolated Power
    spd DECIMAL(4,1),               -- Speed Score
    ubr DECIMAL(4,1),               -- Ultimate Base Running
    wrc DECIMAL(5,1),               -- Weighted Runs Created
    wrc_27 DECIMAL(4,1),            -- Weighted Runs Created per 27 outs
    off DECIMAL(4,1),               -- Offensive Value
    def DECIMAL(4,1),               -- Defensive Value
    war DECIMAL(4,1),               -- Wins Above Replacement
    -- Batted Ball Data
    gb_percent DECIMAL(4,1),        -- Ground Ball %
    fb_percent DECIMAL(4,1),        -- Fly Ball %
    ld_percent DECIMAL(4,1),        -- Line Drive %
    iffb_percent DECIMAL(4,1),      -- Infield Fly Ball %
    hr_fb DECIMAL(4,1),             -- HR/FB ratio
    -- Pitch Selection
    o_swing_percent DECIMAL(4,1),   -- O-Swing %
    z_swing_percent DECIMAL(4,1),   -- Z-Swing %
    swing_percent DECIMAL(4,1),     -- Swing %
    o_contact_percent DECIMAL(4,1), -- O-Contact %
    z_contact_percent DECIMAL(4,1), -- Z-Contact %
    contact_percent DECIMAL(4,1),   -- Contact %
    zone_percent DECIMAL(4,1),      -- Zone %
    f_strike_percent DECIMAL(4,1),  -- F-Strike %
    swstr_percent DECIMAL(4,1),     -- SwStr %
    -- Clutch and Situational
    clutch DECIMAL(4,2),            -- Clutch
    wpa DECIMAL(4,2),               -- Win Probability Added
    re24 DECIMAL(4,1),              -- Run Expectancy 24
    rew DECIMAL(4,1),               -- Run Expectancy Wins
    pli DECIMAL(4,2),               -- Plate appearance Leverage Index
    inlev DECIMAL(4,2),             -- Average Leverage Index
    cents DECIMAL(4,1),             -- Salary in cents
    dollars INTEGER,                -- Salary in dollars
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season)
);

-- FanGraphs Pitching Advanced Metrics
CREATE TABLE IF NOT EXISTS fangraphs_pitching (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    wins INTEGER,
    losses INTEGER,
    saves INTEGER,
    holds INTEGER,
    games INTEGER,
    games_started INTEGER,
    innings_pitched DECIMAL(5,1),
    hits_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    strikeouts INTEGER,
    -- Advanced Metrics
    era DECIMAL(4,2),               -- Earned Run Average
    whip DECIMAL(4,2),              -- Walks + Hits per Inning Pitched
    fip DECIMAL(4,2),               -- Fielding Independent Pitching
    xfip DECIMAL(4,2),              -- Expected Fielding Independent Pitching
    siera DECIMAL(4,2),             -- Skill-Interactive Earned Run Average
    k_9 DECIMAL(4,1),               -- Strikeouts per 9 innings
    bb_9 DECIMAL(4,1),              -- Walks per 9 innings
    hr_9 DECIMAL(4,1),              -- Home runs per 9 innings
    k_bb DECIMAL(4,2),              -- Strikeout-to-walk ratio
    -- Batted Ball Data
    gb_percent DECIMAL(4,1),        -- Ground Ball %
    fb_percent DECIMAL(4,1),        -- Fly Ball %
    ld_percent DECIMAL(4,1),        -- Line Drive %
    iffb_percent DECIMAL(4,1),      -- Infield Fly Ball %
    hr_fb DECIMAL(4,1),             -- HR/FB ratio
    babip DECIMAL(5,3),             -- Batting Average on Balls in Play
    lob_percent DECIMAL(4,1),       -- Left on Base %
    -- Pitch Type Data
    fb_velocity DECIMAL(4,1),       -- Fastball Velocity
    fb_percent_usage DECIMAL(4,1),  -- Fastball Usage %
    sl_percent DECIMAL(4,1),        -- Slider %
    ct_percent DECIMAL(4,1),        -- Cutter %
    cb_percent DECIMAL(4,1),        -- Curveball %
    ch_percent DECIMAL(4,1),        -- Changeup %
    sf_percent DECIMAL(4,1),        -- Split-finger %
    kn_percent DECIMAL(4,1),        -- Knuckleball %
    -- Performance Metrics
    war DECIMAL(4,1),               -- Wins Above Replacement
    wpa DECIMAL(4,2),               -- Win Probability Added
    re24 DECIMAL(4,1),              -- Run Expectancy 24
    rew DECIMAL(4,1),               -- Run Expectancy Wins
    pli DECIMAL(4,2),               -- Plate appearance Leverage Index
    inlev DECIMAL(4,2),             -- Average Leverage Index
    gmli DECIMAL(4,2),              -- Game Leverage Index
    wpa_minus DECIMAL(4,2),         -- WPA allowed
    wpa_plus DECIMAL(4,2),          -- WPA generated
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, season)
);

-- Weather Data for Game Predictions
CREATE TABLE IF NOT EXISTS game_weather (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) NOT NULL,
    venue VARCHAR(100) NOT NULL,
    game_date DATE NOT NULL,
    game_time TIME,
    -- Weather Conditions
    temperature_f INTEGER,          -- Temperature in Fahrenheit
    humidity_percent INTEGER,       -- Humidity percentage
    wind_speed_mph DECIMAL(4,1),    -- Wind speed in MPH
    wind_direction VARCHAR(10),     -- Wind direction (N, NE, E, SE, S, SW, W, NW)
    wind_direction_degrees INTEGER, -- Wind direction in degrees
    barometric_pressure DECIMAL(5,2), -- Pressure in inches of mercury
    weather_condition VARCHAR(50),  -- Clear, Cloudy, Rain, etc.
    precipitation_chance INTEGER,   -- Chance of precipitation %
    precipitation_amount DECIMAL(4,2), -- Precipitation in inches
    cloud_cover_percent INTEGER,    -- Cloud cover percentage
    visibility_miles DECIMAL(4,1),  -- Visibility in miles
    uv_index INTEGER,               -- UV Index
    -- Calculated Wind Factors
    wind_help_factor DECIMAL(4,2),  -- Wind helping/hurting home runs (-1 to 1)
    temperature_factor DECIMAL(4,2), -- Temperature effect on ball carry
    humidity_factor DECIMAL(4,2),   -- Humidity effect on ball carry
    altitude_feet INTEGER,          -- Stadium altitude in feet
    dome_type VARCHAR(20),          -- 'outdoor', 'retractable', 'fixed_dome'
    -- API and Source Info
    weather_api_source VARCHAR(50), -- Which weather API was used
    forecast_hours_ahead INTEGER,   -- How many hours ahead was forecast
    is_forecast BOOLEAN DEFAULT true, -- true if forecast, false if actual
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id)
);

-- Stadium Information for Weather Context
CREATE TABLE IF NOT EXISTS stadium_info (
    id SERIAL PRIMARY KEY,
    stadium_name VARCHAR(100) UNIQUE NOT NULL,
    team VARCHAR(10) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(5),
    country VARCHAR(5) DEFAULT 'USA',
    -- Geographic Data
    latitude DECIMAL(10,7) NOT NULL,
    longitude DECIMAL(10,7) NOT NULL,
    elevation_feet INTEGER,
    timezone VARCHAR(50),
    -- Stadium Characteristics
    capacity INTEGER,
    surface_type VARCHAR(20),       -- 'grass', 'artificial'
    roof_type VARCHAR(20),          -- 'outdoor', 'retractable', 'fixed_dome'
    -- Ballpark Factors
    left_field_distance INTEGER,    -- Distance to left field foul pole
    center_field_distance INTEGER,  -- Distance to center field
    right_field_distance INTEGER,   -- Distance to right field foul pole
    left_field_height INTEGER,      -- Height of left field wall
    right_field_height INTEGER,     -- Height of right field wall
    foul_territory_factor DECIMAL(4,2), -- Relative foul territory (1.0 = average)
    -- Historical Park Factors
    park_factor_runs DECIMAL(4,2),  -- Historical run scoring factor
    park_factor_hr DECIMAL(4,2),    -- Historical home run factor
    park_factor_hits DECIMAL(4,2),  -- Historical hits factor
    park_factor_walks DECIMAL(4,2), -- Historical walks factor
    park_factor_so DECIMAL(4,2),    -- Historical strikeouts factor
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Player Matchup Analysis (Batter vs Pitcher)
CREATE TABLE IF NOT EXISTS player_matchups (
    id SERIAL PRIMARY KEY,
    batter_id VARCHAR(50) NOT NULL,
    pitcher_id VARCHAR(50) NOT NULL,
    season INTEGER NOT NULL,
    -- Matchup Statistics
    plate_appearances INTEGER DEFAULT 0,
    at_bats INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    home_runs INTEGER DEFAULT 0,
    walks INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    hit_by_pitch INTEGER DEFAULT 0,
    sacrifice_flies INTEGER DEFAULT 0,
    -- Advanced Matchup Metrics
    batting_avg DECIMAL(5,3),
    on_base_pct DECIMAL(5,3),
    slugging_pct DECIMAL(5,3),
    ops DECIMAL(5,3),
    woba DECIMAL(5,3),
    -- Situational Context
    runners_in_scoring_pos_hits INTEGER DEFAULT 0,
    runners_in_scoring_pos_abs INTEGER DEFAULT 0,
    late_close_hits INTEGER DEFAULT 0,
    late_close_abs INTEGER DEFAULT 0,
    -- Metadata
    last_matchup_date DATE,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batter_id, pitcher_id, season)
);

-- Create indexes for optimal query performance
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_player_season ON fangraphs_batting(player_id, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_team_season ON fangraphs_batting(team, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_war ON fangraphs_batting(war DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_wrc_plus ON fangraphs_batting(wrc_plus DESC);

CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_player_season ON fangraphs_pitching(player_id, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_team_season ON fangraphs_pitching(team, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_war ON fangraphs_pitching(war DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_fip ON fangraphs_pitching(fip);

CREATE INDEX IF NOT EXISTS idx_game_weather_game_id ON game_weather(game_id);
CREATE INDEX IF NOT EXISTS idx_game_weather_date ON game_weather(game_date);
CREATE INDEX IF NOT EXISTS idx_game_weather_venue ON game_weather(venue);
CREATE INDEX IF NOT EXISTS idx_game_weather_temperature ON game_weather(temperature_f);

CREATE INDEX IF NOT EXISTS idx_stadium_info_team ON stadium_info(team);
CREATE INDEX IF NOT EXISTS idx_stadium_info_city ON stadium_info(city);

CREATE INDEX IF NOT EXISTS idx_player_matchups_batter ON player_matchups(batter_id, season);
CREATE INDEX IF NOT EXISTS idx_player_matchups_pitcher ON player_matchups(pitcher_id, season);
CREATE INDEX IF NOT EXISTS idx_player_matchups_season ON player_matchups(season);

-- Create triggers for updated_at columns
CREATE TRIGGER update_fangraphs_batting_updated_at BEFORE UPDATE ON fangraphs_batting
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fangraphs_pitching_updated_at BEFORE UPDATE ON fangraphs_pitching
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_game_weather_updated_at BEFORE UPDATE ON game_weather
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stadium_info_updated_at BEFORE UPDATE ON stadium_info
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_player_matchups_updated_at BEFORE UPDATE ON player_matchups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial collection status entries for new data types
INSERT INTO collection_status (collection_type, status, started_at) VALUES 
('fangraphs_batting', 'pending', CURRENT_TIMESTAMP),
('fangraphs_pitching', 'pending', CURRENT_TIMESTAMP),
('game_weather', 'pending', CURRENT_TIMESTAMP),
('stadium_info', 'pending', CURRENT_TIMESTAMP),
('player_matchups', 'pending', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

-- Create useful views for prediction modeling
CREATE OR REPLACE VIEW prediction_ready_batters AS
SELECT 
    fb.player_id,
    fb.player_name,
    fb.team,
    fb.season,
    -- Core Stats
    fb.games, fb.plate_appearances, fb.home_runs, fb.rbi,
    -- Advanced Metrics for Predictions
    fb.woba, fb.wrc_plus, fb.babip, fb.iso, fb.war,
    -- Batted Ball Profile
    fb.gb_percent, fb.fb_percent, fb.ld_percent, fb.hr_fb,
    -- Plate Discipline
    fb.o_swing_percent, fb.z_swing_percent, fb.contact_percent, fb.swstr_percent,
    -- Situational
    fb.clutch, fb.wpa, fb.re24,
    -- Recent Form
    fb.updated_at
FROM fangraphs_batting fb
WHERE fb.season = EXTRACT(YEAR FROM CURRENT_DATE)
  AND fb.plate_appearances >= 50
ORDER BY fb.wrc_plus DESC;

CREATE OR REPLACE VIEW prediction_ready_pitchers AS
SELECT 
    fp.player_id,
    fp.player_name,
    fp.team,
    fp.season,
    -- Core Stats
    fp.games, fp.games_started, fp.innings_pitched, fp.strikeouts,
    -- Advanced Metrics for Predictions
    fp.fip, fp.xfip, fp.siera, fp.war, fp.k_9, fp.bb_9, fp.hr_9,
    -- Batted Ball Profile
    fp.gb_percent, fp.fb_percent, fp.ld_percent, fp.hr_fb, fp.babip,
    -- Pitch Mix
    fp.fb_velocity, fp.fb_percent_usage, fp.sl_percent, fp.ch_percent,
    -- Performance
    fp.wpa, fp.re24,
    -- Recent Form
    fp.updated_at
FROM fangraphs_pitching fp
WHERE fp.season = EXTRACT(YEAR FROM CURRENT_DATE)
  AND fp.innings_pitched >= 10
ORDER BY fp.war DESC;

CREATE OR REPLACE VIEW game_prediction_data AS
SELECT 
    g.game_id,
    g.game_date,
    g.home_team,
    g.away_team,
    g.venue,
    g.game_time,
    -- Weather Data
    gw.temperature_f,
    gw.wind_speed_mph,
    gw.wind_direction,
    gw.humidity_percent,
    gw.weather_condition,
    gw.wind_help_factor,
    gw.temperature_factor,
    -- Stadium Factors
    si.elevation_feet,
    si.park_factor_runs,
    si.park_factor_hr,
    si.roof_type,
    si.foul_territory_factor,
    -- Game Status
    g.game_status,
    g.updated_at
FROM games g
LEFT JOIN game_weather gw ON g.game_id = gw.game_id
LEFT JOIN stadium_info si ON g.venue = si.stadium_name
WHERE g.game_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY g.game_date DESC, g.game_time;
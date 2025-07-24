-- MLB Data Service Database Schema
-- Initialize tables for MLB data storage with proper indexing

-- Players table - stores active MLB player information
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) UNIQUE NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    team VARCHAR(10),
    position VARCHAR(10),
    batting_avg DECIMAL(4,3),
    home_runs INTEGER,
    rbi INTEGER,
    ops DECIMAL(5,3),
    war DECIMAL(4,1),
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Games table - stores MLB game information
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50) UNIQUE NOT NULL,
    game_date DATE NOT NULL,
    home_team VARCHAR(10) NOT NULL,
    away_team VARCHAR(10) NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    game_status VARCHAR(20),
    venue VARCHAR(100),
    game_time VARCHAR(20),
    inning INTEGER,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Statcast table - stores pitch-by-pitch data
CREATE TABLE IF NOT EXISTS statcast (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(50),
    player_name VARCHAR(100),
    player_id VARCHAR(50),
    events VARCHAR(50),
    description VARCHAR(200),
    launch_speed DECIMAL(5,1),
    launch_angle DECIMAL(5,1),
    hit_distance_sc DECIMAL(6,1),
    exit_velocity DECIMAL(5,1),
    pitch_type VARCHAR(10),
    release_speed DECIMAL(5,1),
    game_date DATE,
    at_bat_number INTEGER,
    pitch_number INTEGER,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Collection status table - tracks data collection jobs
CREATE TABLE IF NOT EXISTS collection_status (
    id SERIAL PRIMARY KEY,
    collection_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed'
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    records_collected INTEGER DEFAULT 0,
    error_message TEXT,
    data_source VARCHAR(50)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);
CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);
CREATE INDEX IF NOT EXISTS idx_players_updated_at ON players(updated_at);

CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team);
CREATE INDEX IF NOT EXISTS idx_games_status ON games(game_status);

CREATE INDEX IF NOT EXISTS idx_statcast_player ON statcast(player_name);
CREATE INDEX IF NOT EXISTS idx_statcast_game_date ON statcast(game_date);
CREATE INDEX IF NOT EXISTS idx_statcast_events ON statcast(events);

CREATE INDEX IF NOT EXISTS idx_collection_status_type ON collection_status(collection_type);
CREATE INDEX IF NOT EXISTS idx_collection_status_started ON collection_status(started_at);

-- Create a function to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at
CREATE TRIGGER update_players_updated_at BEFORE UPDATE ON players
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_games_updated_at BEFORE UPDATE ON games
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial collection status entries
INSERT INTO collection_status (collection_type, status, started_at) VALUES 
('players', 'pending', CURRENT_TIMESTAMP),
('games', 'pending', CURRENT_TIMESTAMP),
('statcast', 'pending', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

-- Create a view for today's games with team names
CREATE OR REPLACE VIEW todays_games AS
SELECT 
    game_id,
    game_date,
    home_team,
    away_team,
    home_score,
    away_score,
    game_status,
    venue,
    game_time,
    inning,
    data_source,
    updated_at
FROM games 
WHERE game_date = CURRENT_DATE
ORDER BY game_time;

-- Create a view for active players with recent stats
CREATE OR REPLACE VIEW active_players AS
SELECT 
    player_id,
    full_name,
    team,
    position,
    batting_avg,
    home_runs,
    rbi,
    ops,
    war,
    data_source,
    updated_at
FROM players 
WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY war DESC NULLS LAST;

-- Load FanGraphs and Weather schema extensions
\i /docker-entrypoint-initdb.d/fangraphs_weather_schema.sql
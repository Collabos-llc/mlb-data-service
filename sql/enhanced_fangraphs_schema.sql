-- Enhanced FanGraphs Schema with Comprehensive Metrics
-- =====================================================

-- Drop existing tables to recreate with full schema
DROP TABLE IF EXISTS fangraphs_batting CASCADE;
DROP TABLE IF EXISTS fangraphs_pitching CASCADE;

-- Enhanced FanGraphs Batting Table (100+ most critical fields)
CREATE TABLE IF NOT EXISTS fangraphs_batting (
    id SERIAL PRIMARY KEY,
    
    -- Basic Identifiers
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    
    -- Traditional Stats
    games INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    singles INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hit_by_pitch INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    grounded_double_plays INTEGER,
    
    -- Rate Stats
    batting_avg DECIMAL(5,3),
    on_base_pct DECIMAL(5,3),
    slugging_pct DECIMAL(5,3),
    ops DECIMAL(5,3),
    
    -- Advanced Sabermetrics
    woba DECIMAL(5,3),              -- Weighted On-Base Average
    wrc_plus INTEGER,               -- Weighted Runs Created Plus
    babip DECIMAL(5,3),             -- Batting Average on Balls in Play
    iso DECIMAL(5,3),               -- Isolated Power
    spd DECIMAL(4,1),               -- Speed Score
    ubr DECIMAL(4,1),               -- Ultimate Base Running
    wrc DECIMAL(5,1),               -- Weighted Runs Created
    wrc_27 DECIMAL(4,1),            -- wRC per 27 outs
    off DECIMAL(4,1),               -- Offensive Runs Above Average
    def DECIMAL(4,1),               -- Defensive Runs Above Average
    war DECIMAL(4,1),               -- Wins Above Replacement
    
    -- Expected Stats (Statcast Integration)
    expected_ba DECIMAL(5,3),       -- Expected Batting Average
    expected_slg DECIMAL(5,3),      -- Expected Slugging
    expected_woba DECIMAL(5,3),     -- Expected wOBA
    expected_ops DECIMAL(5,3),      -- Expected OPS
    
    -- Statcast Metrics
    barrel_pct DECIMAL(4,1),        -- Barrel Percentage
    max_exit_velocity DECIMAL(5,1), -- Maximum Exit Velocity
    avg_exit_velocity DECIMAL(5,1), -- Average Exit Velocity
    avg_launch_angle DECIMAL(4,1),  -- Average Launch Angle
    hard_hit_pct DECIMAL(4,1),      -- Hard Hit Percentage
    sweet_spot_pct DECIMAL(4,1),    -- Sweet Spot Percentage
    
    -- Batted Ball Data
    gb_percent DECIMAL(4,1),        -- Ground Ball Percentage
    fb_percent DECIMAL(4,1),        -- Fly Ball Percentage
    ld_percent DECIMAL(4,1),        -- Line Drive Percentage
    iffb_percent DECIMAL(4,1),      -- Infield Fly Ball Percentage
    hr_fb DECIMAL(4,1),             -- Home Run per Fly Ball
    pull_percent DECIMAL(4,1),      -- Pull Percentage
    cent_percent DECIMAL(4,1),      -- Center Percentage
    oppo_percent DECIMAL(4,1),      -- Opposite Field Percentage
    soft_percent DECIMAL(4,1),      -- Soft Contact Percentage
    med_percent DECIMAL(4,1),       -- Medium Contact Percentage
    hard_percent DECIMAL(4,1),      -- Hard Contact Percentage
    
    -- Plate Discipline
    o_swing_percent DECIMAL(4,1),   -- Outside Zone Swing%
    z_swing_percent DECIMAL(4,1),   -- Inside Zone Swing%
    swing_percent DECIMAL(4,1),     -- Overall Swing%
    o_contact_percent DECIMAL(4,1), -- Outside Zone Contact%
    z_contact_percent DECIMAL(4,1), -- Inside Zone Contact%
    contact_percent DECIMAL(4,1),   -- Overall Contact%
    zone_percent DECIMAL(4,1),      -- Zone%
    f_strike_percent DECIMAL(4,1),  -- First Pitch Strike%
    swstr_percent DECIMAL(4,1),     -- Swinging Strike%
    called_strike_pct DECIMAL(4,1), -- Called Strike%
    csw_percent DECIMAL(4,1),       -- Called Strike + Whiff%
    
    -- Advanced Plate Discipline
    chase_rate DECIMAL(4,1),        -- Chase Rate (O-Swing%)
    whiff_rate DECIMAL(4,1),        -- Whiff Rate
    in_zone_whiff_rate DECIMAL(4,1), -- In-Zone Whiff Rate
    out_zone_whiff_rate DECIMAL(4,1), -- Out-Zone Whiff Rate
    
    -- Situational Stats
    clutch DECIMAL(4,2),            -- Clutch Score
    wpa DECIMAL(4,2),               -- Win Probability Added
    re24 DECIMAL(4,1),              -- Run Expectancy Based on 24 Base States
    rew DECIMAL(4,1),               -- Runs Above Average based on RE24
    pli DECIMAL(4,2),               -- Average Leverage Index
    inlev DECIMAL(4,2),             -- Leverage Index - Innings
    gmli DECIMAL(4,2),              -- Game-tying/go-ahead Leverage Index
    wpa_plus DECIMAL(4,2),          -- Positive WPA
    wpa_minus DECIMAL(4,2),         -- Negative WPA
    
    -- Count-Specific Stats
    bb_rate DECIMAL(4,1),           -- Walk Rate (BB%)
    k_rate DECIMAL(4,1),            -- Strikeout Rate (K%)
    bb_k_ratio DECIMAL(4,2),        -- Walk to Strikeout Ratio
    
    -- Advanced Ratios
    iso_siso DECIMAL(4,3),          -- ISO / Secondary ISO
    sec_avg DECIMAL(5,3),           -- Secondary Average
    eye_contact DECIMAL(4,2),       -- Eye (BB/K) * Contact%
    
    -- Value Metrics
    dollars INTEGER,                -- Dollar Value
    cents DECIMAL(4,1),             -- Cents per PA
    
    -- Quality of Contact
    ev95_percent DECIMAL(4,1),      -- Exit Velocity 95+ mph%
    brl_percent DECIMAL(4,1),       -- Barrel Rate%
    brl_pa DECIMAL(4,1),            -- Barrels per PA
    
    -- Sprint Speed (if available)
    sprint_speed DECIMAL(4,1),      -- Sprint Speed (ft/sec)
    
    -- Team Context
    team_pa_pct DECIMAL(4,1),       -- Percentage of Team PA
    
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(player_id, season)
);

-- Enhanced FanGraphs Pitching Table (100+ most critical fields)
CREATE TABLE IF NOT EXISTS fangraphs_pitching (
    id SERIAL PRIMARY KEY,
    
    -- Basic Identifiers
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    team VARCHAR(10) NOT NULL,
    season INTEGER NOT NULL,
    
    -- Traditional Stats
    wins INTEGER,
    losses INTEGER,
    saves INTEGER,
    holds INTEGER,
    games INTEGER,
    games_started INTEGER,
    complete_games INTEGER,
    shutouts INTEGER,
    innings_pitched DECIMAL(6,1),
    hits_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    intentional_walks INTEGER,
    hit_batsmen INTEGER,
    strikeouts INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    
    -- Rate Stats
    era DECIMAL(5,2),               -- Earned Run Average
    whip DECIMAL(4,2),              -- Walks + Hits per IP
    
    -- Advanced ERA Estimators
    fip DECIMAL(5,2),               -- Fielding Independent Pitching
    xfip DECIMAL(5,2),              -- Expected FIP
    siera DECIMAL(4,2),             -- Skill Interactive ERA
    expected_era DECIMAL(5,2),      -- Expected ERA (Statcast)
    
    -- Rate Stats per 9 innings
    k_9 DECIMAL(4,1),               -- Strikeouts per 9
    bb_9 DECIMAL(4,1),              -- Walks per 9
    hr_9 DECIMAL(4,1),              -- Home Runs per 9
    h_9 DECIMAL(4,1),               -- Hits per 9
    
    -- Ratios
    k_bb DECIMAL(4,2),              -- Strikeout to Walk Ratio
    k_bb_percent DECIMAL(4,1),      -- K% - BB%
    
    -- Rate Percentages
    k_percent DECIMAL(4,1),         -- Strikeout Rate
    bb_percent DECIMAL(4,1),        -- Walk Rate
    
    -- Batted Ball Data
    gb_percent DECIMAL(4,1),        -- Ground Ball Percentage
    fb_percent DECIMAL(4,1),        -- Fly Ball Percentage
    ld_percent DECIMAL(4,1),        -- Line Drive Percentage
    iffb_percent DECIMAL(4,1),      -- Infield Fly Ball Percentage
    hr_fb DECIMAL(4,1),             -- Home Run per Fly Ball
    babip DECIMAL(5,3),             -- BABIP Against
    lob_percent DECIMAL(4,1),       -- Left on Base Percentage
    
    -- Contact Quality Against
    pull_percent DECIMAL(4,1),      -- Pull% Against
    cent_percent DECIMAL(4,1),      -- Center% Against
    oppo_percent DECIMAL(4,1),      -- Opposite Field% Against
    soft_percent DECIMAL(4,1),      -- Soft Contact% Against
    med_percent DECIMAL(4,1),       -- Medium Contact% Against
    hard_percent DECIMAL(4,1),      -- Hard Contact% Against
    
    -- Expected Stats Against
    expected_ba_against DECIMAL(5,3), -- Expected BA Against
    expected_slg_against DECIMAL(5,3), -- Expected SLG Against
    expected_woba_against DECIMAL(5,3), -- Expected wOBA Against
    
    -- Statcast Metrics Against
    barrel_percent_against DECIMAL(4,1), -- Barrel% Against
    max_exit_velo_against DECIMAL(5,1),  -- Max Exit Velocity Against
    avg_exit_velo_against DECIMAL(5,1),  -- Avg Exit Velocity Against
    hard_hit_percent_against DECIMAL(4,1), -- Hard Hit% Against
    sweet_spot_percent_against DECIMAL(4,1), -- Sweet Spot% Against
    
    -- Plate Discipline Against
    o_swing_percent DECIMAL(4,1),   -- Outside Zone Swing% Against
    z_swing_percent DECIMAL(4,1),   -- Inside Zone Swing% Against
    swing_percent DECIMAL(4,1),     -- Overall Swing% Against
    o_contact_percent DECIMAL(4,1), -- Outside Zone Contact% Against
    z_contact_percent DECIMAL(4,1), -- Inside Zone Contact% Against
    contact_percent DECIMAL(4,1),   -- Overall Contact% Against
    zone_percent DECIMAL(4,1),      -- Zone% Against
    f_strike_percent DECIMAL(4,1),  -- First Pitch Strike%
    swstr_percent DECIMAL(4,1),     -- Swinging Strike%
    called_strike_pct DECIMAL(4,1), -- Called Strike%
    csw_percent DECIMAL(4,1),       -- Called Strike + Whiff%
    
    -- Advanced Command
    strike_percent DECIMAL(4,1),    -- Strike%
    ball_percent DECIMAL(4,1),      -- Ball%
    pace DECIMAL(4,1),              -- Pace (seconds between pitches)
    
    -- Pitch Type Usage (Percentages)
    fb_percent_usage DECIMAL(4,1),  -- Fastball%
    sl_percent DECIMAL(4,1),        -- Slider%
    ct_percent DECIMAL(4,1),        -- Cutter%
    cb_percent DECIMAL(4,1),        -- Curveball%
    ch_percent DECIMAL(4,1),        -- Changeup%
    sf_percent DECIMAL(4,1),        -- Split Finger%
    kn_percent DECIMAL(4,1),        -- Knuckleball%
    
    -- Pitch Velocities
    fb_velocity DECIMAL(4,1),       -- Average Fastball Velocity
    sl_velocity DECIMAL(4,1),       -- Average Slider Velocity
    ct_velocity DECIMAL(4,1),       -- Average Cutter Velocity
    cb_velocity DECIMAL(4,1),       -- Average Curveball Velocity
    ch_velocity DECIMAL(4,1),       -- Average Changeup Velocity
    
    -- Pitch Movement (inches)
    fb_spin_rate INTEGER,           -- Fastball Spin Rate
    sl_spin_rate INTEGER,           -- Slider Spin Rate
    cb_spin_rate INTEGER,           -- Curveball Spin Rate
    ch_spin_rate INTEGER,           -- Changeup Spin Rate
    
    -- Pitch Results
    fb_whiff_percent DECIMAL(4,1),  -- Fastball Whiff%
    sl_whiff_percent DECIMAL(4,1),  -- Slider Whiff%
    cb_whiff_percent DECIMAL(4,1),  -- Curveball Whiff%
    ch_whiff_percent DECIMAL(4,1),  -- Changeup Whiff%
    
    -- Situational Stats
    clutch DECIMAL(4,2),            -- Clutch Score
    wpa DECIMAL(4,2),               -- Win Probability Added
    re24 DECIMAL(4,1),              -- Run Expectancy Based on 24 Base States
    rew DECIMAL(4,1),               -- Runs Above Average based on RE24
    pli DECIMAL(4,2),               -- Average Leverage Index
    inlev DECIMAL(4,2),             -- Leverage Index - Innings
    gmli DECIMAL(4,2),              -- Game-tying/go-ahead Leverage Index
    wpa_minus DECIMAL(4,2),         -- Negative WPA
    wpa_plus DECIMAL(4,2),          -- Positive WPA
    
    -- Quality Starts and Game Stats
    quality_starts INTEGER,         -- Quality Starts
    quality_start_pct DECIMAL(4,1), -- Quality Start%
    
    -- Performance Metrics
    war DECIMAL(4,1),               -- Wins Above Replacement
    
    -- Value Metrics
    dollars INTEGER,                -- Dollar Value
    
    -- Team Context
    team_ip_pct DECIMAL(4,1),       -- Percentage of Team IP
    
    -- Metadata
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(player_id, season)
);

-- Comprehensive indexes for performance
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_player_season ON fangraphs_batting(player_id, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_team_season ON fangraphs_batting(team, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_war ON fangraphs_batting(war DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_wrc_plus ON fangraphs_batting(wrc_plus DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_barrel_pct ON fangraphs_batting(barrel_pct DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_batting_enhanced_expected_woba ON fangraphs_batting(expected_woba DESC);

CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_player_season ON fangraphs_pitching(player_id, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_team_season ON fangraphs_pitching(team, season);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_war ON fangraphs_pitching(war DESC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_fip ON fangraphs_pitching(fip ASC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_siera ON fangraphs_pitching(siera ASC);
CREATE INDEX IF NOT EXISTS idx_fangraphs_pitching_enhanced_k_percent ON fangraphs_pitching(k_percent DESC);

-- Update triggers
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_fangraphs_batting_enhanced_updated_at 
    BEFORE UPDATE ON fangraphs_batting 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fangraphs_pitching_enhanced_updated_at 
    BEFORE UPDATE ON fangraphs_pitching 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
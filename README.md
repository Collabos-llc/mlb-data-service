# MLB Data Service - Enhanced Analytics Platform

A comprehensive, production-ready MLB analytics platform with advanced sabermetrics, Statcast data, and unified player lookup capabilities. Built with Flask, PostgreSQL, and Docker.

## 🚀 Features

- **🗄️ Comprehensive Database**: 472K+ Statcast records, 1.3K+ FanGraphs batting/pitching stats
- **🔑 Unified Player Lookup**: 25,815 players with cross-system ID mappings
- **📊 Advanced Analytics**: 320+ FanGraphs batting fields, 393+ pitching fields, 118+ Statcast fields
- **🌐 REST API**: 11+ endpoints for data collection, retrieval, and player lookup
- **🐳 Production Docker**: Containerized with health checks and monitoring
- **⚡ High Performance**: Optimized queries with database indexing

## 🚀 Quick Start

```bash
# Start the enhanced service
git clone <repo-url>
cd mlb-data-service
docker-compose up --build -d

# Verify service health
curl http://localhost:8101/health

# Search for a player
curl "http://localhost:8101/api/v1/player/search?q=Aaron%20Judge"

# Get unified player profile
curl "http://localhost:8101/api/v1/player/profile?name=Shohei%20Ohtani"

# Get FanGraphs batting data  
curl "http://localhost:8101/api/v1/fangraphs/batting?season=2025&limit=5"

# Get Statcast data
curl "http://localhost:8101/api/v1/statcast?limit=5"
```

## 📡 API Endpoints

### Core Service
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check for container orchestration |
| GET | `/api/v1/status` | Service status and comprehensive database statistics |

### Player Lookup & Profiles
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/player/search` | Search players across all systems |
| GET | `/api/v1/player/profile` | Unified player profile (FanGraphs + Statcast) |
| GET | `/api/v1/player/ids` | Player ID mappings across all systems |

### Advanced Analytics Data
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/fangraphs/batting` | FanGraphs batting statistics (320+ fields) |
| GET | `/api/v1/statcast` | Statcast pitch-level data (118+ fields) |
| GET | `/api/v1/analytics/summary` | Comprehensive analytics overview |

### Data Collection
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/collect/fangraphs/batting` | Collect FanGraphs batting data |
| POST | `/api/v1/collect/fangraphs/pitching` | Collect FanGraphs pitching data |
| POST | `/api/v1/collect/statcast` | Collect Statcast data |

## 🏗️ Architecture

```
mlb-data-service/
├── mlb_data_service/
│   ├── enhanced_app.py           # Enhanced Flask application
│   ├── enhanced_database.py     # Advanced database manager
│   ├── app.py                   # Legacy Flask application
│   └── external_apis.py         # External API integration
├── sql/                         # Database schema and migrations
├── logs/                        # Application logs
├── docker-compose.yml           # Service orchestration with PostgreSQL
├── Dockerfile                   # Container configuration
├── requirements.txt             # Python dependencies
├── player_lookup_analysis.py    # Player ID mapping analysis
├── PLAYER_LOOKUP_INTEGRATION.md # Comprehensive integration guide
└── migration scripts/           # Database migration utilities
```

## 🗄️ Database Schema

### Core Tables
- **`player_lookup`** (25,815 records): Master player registry with cross-system IDs
- **`fangraphs_batting`** (1,323 records): 320+ advanced batting metrics
- **`fangraphs_pitching`** (765 records): 393+ advanced pitching metrics  
- **`fangraphs_fielding`** (1,976 records): 63+ defensive statistics
- **`statcast`** (472,395 records): 118+ pitch-level analytics

### Key Relationships
- `player_lookup.key_fangraphs` → `fangraphs_batting."IDfg"`
- `player_lookup.key_mlbam` → `statcast.batter/pitcher`
- Optimized indexes for cross-system queries

## 🔗 Data Sources

- **FanGraphs**: Advanced sabermetrics (wOBA, wRC+, WAR, FIP, xFIP)
- **Statcast**: Pitch tracking (exit velocity, launch angle, spin rate)
- **MLB Advanced Media**: Player identification and game context
- **Baseball Reference**: Historical player data and career spans

## 💻 Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run player lookup analysis
python3 player_lookup_analysis.py

# Run migration validation
python3 statcast_migration_report.py

# Database direct access
PGPASSWORD=mlb_secure_pass_2024 psql -h localhost -p 5439 -U mlb_user -d mlb_data
```

## 🚀 Production Deployment

The service is production-ready with:
- **Health check endpoint** for Kubernetes/Docker Swarm
- **Comprehensive logging** for monitoring and debugging
- **Optimized database** with indexes and connection pooling
- **CORS configuration** for cross-origin requests
- **Error handling** and graceful failure recovery
- **Environment-based configuration** for different deployment stages

## 📊 Analytics Capabilities

### Advanced Metrics Available
- **Batting**: wOBA, wRC+, WAR, Barrel%, xwOBA, Exit Velocity (320+ fields)
- **Pitching**: FIP, xFIP, SIERA, K%, BB%, WHIP (393+ fields)
- **Statcast**: Spin rate, launch angle, expected outcomes, movement data (118+ fields)

### Cross-System Analysis
```bash
# Find top performers with both FanGraphs and Statcast data
curl "http://localhost:8101/api/v1/player/profile?name=Aaron%20Judge"

# Search players across all systems
curl "http://localhost:8101/api/v1/player/search?q=Ohtani"

# Get comprehensive analytics overview
curl "http://localhost:8101/api/v1/analytics/summary"
```

## 🔍 Documentation

- **[PLAYER_LOOKUP_INTEGRATION.md](PLAYER_LOOKUP_INTEGRATION.md)**: Complete player ID mapping guide
- **player_lookup_analysis.py**: Automated analysis and validation scripts
- **API Documentation**: All endpoints documented with examples above

## 📈 Monitoring

Monitor service health and performance:
```bash
# Check enhanced service logs
docker logs enhanced-mlb-test -f

# Service statistics with database metrics
curl http://localhost:8101/api/v1/status

# Health monitoring
curl http://localhost:8101/health

# Database connection test
curl "http://localhost:8101/api/v1/analytics/summary"
```

## 🎯 Use Cases

- **Sabermetric Analysis**: Combine traditional and advanced metrics
- **Player Evaluation**: Unified profiles across all data sources
- **Pitch Analysis**: Detailed Statcast data with player context
- **Historical Research**: Cross-reference players across systems
- **Fantasy Baseball**: Advanced metrics for player evaluation
- **Broadcasting/Media**: Rich player data for content creation
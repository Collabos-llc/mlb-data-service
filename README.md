# MLB Data Service

A containerized microservice for collecting and serving MLB data via REST API endpoints. Built with Flask and Docker, integrating with PyBaseball and MLB Stats API.

## Features

- **8 REST API endpoints** for data collection and retrieval
- **External API integration** with PyBaseball, FanGraphs, and MLB Stats API  
- **Docker containerization** with health checks and monitoring
- **Rate limiting** and fallback strategies for API reliability
- **Production-ready** logging and error handling

## Quick Start

```bash
# Clone and start the service
git clone <repo-url>
cd mlb-data-service
docker-compose up --build -d

# Verify service health
curl http://localhost:8001/health

# Collect player data
curl -X POST http://localhost:8001/api/v1/collect/players \
  -H 'Content-Type: application/json' \
  -d '{"limit": 10}'

# Retrieve collected data
curl http://localhost:8001/api/v1/players
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check for container orchestration |
| GET | `/api/v1/status` | Service status and collection statistics |
| GET | `/api/v1/players` | Retrieve collected player data |
| GET | `/api/v1/games/today` | Retrieve today's MLB games |
| GET | `/api/v1/statcast` | Retrieve Statcast data |
| POST | `/api/v1/collect/players` | Trigger player data collection |
| POST | `/api/v1/collect/games` | Trigger games data collection |
| POST | `/api/v1/collect/statcast` | Trigger Statcast data collection |

## Architecture

```
mlb-data-service/
├── mlb_data_service/
│   ├── app.py              # Main Flask application
│   └── external_apis.py    # External API integration
├── logs/                   # Application logs
├── Dockerfile             # Container configuration
├── docker-compose.yml     # Service orchestration
├── requirements.txt       # Python dependencies
├── test_mlb_service.py    # End-to-end test suite
├── validate_service.py    # Service validation
└── demo_complete_system.py # Complete demo
```

## External APIs

- **PyBaseball**: FanGraphs batting/pitching statistics and Statcast data
- **MLB Stats API**: Games, schedules, scores, and team information
- **Rate limiting**: Intelligent throttling to prevent API abuse
- **Fallback data**: Offline development support

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run validation
python validate_service.py

# Run tests (requires service running)
python test_mlb_service.py

# View demo
python demo_complete_system.py
```

## Production Deployment

The service is production-ready with:
- Health check endpoint for Kubernetes/Docker Swarm
- Structured logging for monitoring
- CORS configuration for cross-origin requests
- Error handling and graceful failures
- Environment-based configuration

## Integration

This microservice is designed to integrate with:
- **Prediction Engine** (Port 8002) - Consumes MLB data
- **Content Creation Service** (Port 8003) - Uses predictions
- **Social Media Service** (Port 8004) - Publishes content

## Monitoring

Monitor service health and performance:
```bash
# Check logs
docker-compose logs -f mlb-data-service

# Service statistics
curl http://localhost:8001/api/v1/status

# Health monitoring
curl http://localhost:8001/health
```
# MLB Data Service - Production Deployment Guide

## Overview

This guide covers the complete production deployment of the MLB Data Service with integrated monitoring stack. The deployment includes:

- **Core Application**: MLB Data Service with real-time data collection
- **Database**: PostgreSQL with optimized configuration
- **Monitoring**: Prometheus, Grafana, Alertmanager
- **Logging**: Loki and Promtail for centralized log management
- **System Metrics**: Node Exporter for infrastructure monitoring

## Quick Start

### 1. Deploy Production Stack

```bash
# Run the automated deployment script
python3 deploy_production.py
```

### 2. Access Services

After successful deployment, access the following services:

- **MLB Data Service**: http://localhost:8101
- **Grafana Dashboard**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Alertmanager**: http://localhost:9093

## Architecture

### Services Overview

| Service | Port | Purpose |
|---------|------|---------|
| mlb-data-service | 8101 | Main application API |
| postgres | 5439 | Primary database |
| prometheus | 9090 | Metrics collection |
| grafana | 3000 | Visualization dashboards |
| alertmanager | 9093 | Alert management |
| node-exporter | 9100 | System metrics |
| loki | 3100 | Log aggregation |
| promtail | - | Log shipping |

### Network Architecture

All services run in a dedicated Docker network (`mlb-network`) with the following connectivity:

```
Internet → Load Balancer → MLB Data Service → PostgreSQL
                       ↓
                   Prometheus ← Node Exporter
                       ↓
                   Grafana
                       ↓
                 Alertmanager
                       ↓
                    Loki ← Promtail
```

## Configuration

### Environment Variables

Production environment is configured via `.env.production`:

```bash
# Application Settings
FLASK_ENV=production
LOG_LEVEL=INFO
MONITORING_ENABLED=true

# Database Configuration
POSTGRES_DB=mlb_data
POSTGRES_USER=mlb_user
POSTGRES_PASSWORD=mlb_secure_pass_2024
POSTGRES_PORT=5439

# Service Ports
SERVICE_PORT=8101
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
ALERTMANAGER_PORT=9093
NODE_EXPORTER_PORT=9100
LOKI_PORT=3100

# Security
GRAFANA_ADMIN_PASSWORD=admin123
```

### Production Configuration

Key production settings in `config/production.yaml`:

- **Database Pool**: 20 connections with 30 overflow
- **Logging**: Structured JSON logs with rotation
- **Monitoring**: Comprehensive metrics and alerts
- **Rate Limiting**: API rate limits and timeouts
- **Security**: CORS, authentication, SSL configurations

## Monitoring & Alerting

### Metrics Collected

1. **Application Metrics**:
   - Request rate and response times
   - Error rates and status codes
   - Data collection success/failure rates
   - Database connection pool usage

2. **System Metrics**:
   - CPU, Memory, Disk usage
   - Network I/O
   - Container resource utilization

3. **Business Metrics**:
   - Daily data collection counts
   - API endpoint usage
   - Scheduler job status

### Alert Rules

The system includes pre-configured alerts for:

- **Critical**: Service down, high error rate, database failures
- **Warning**: High response time, memory usage, disk space
- **Info**: Data collection failures, scheduler issues

### Dashboards

Grafana includes dashboards for:

1. **Service Overview**: Health, request rates, response times
2. **Infrastructure**: System metrics, resource usage
3. **Application**: Business metrics, data collection status
4. **Alerts**: Active alerts and alert history

## API Endpoints

### Health & Status

- `GET /health` - Service health check
- `GET /api/v1/status` - Detailed service status
- `GET /metrics` - Prometheus metrics

### Data Collection

- `POST /api/v1/collect/players` - Trigger player data collection
- `POST /api/v1/collect/games` - Trigger games data collection
- `POST /api/v1/collect/statcast` - Trigger Statcast data collection

### Data Retrieval

- `GET /api/v1/players` - Get player data
- `GET /api/v1/games/today` - Get today's games
- `GET /api/v1/statcast` - Get Statcast data

### Scheduler

- `GET /api/v1/scheduler/status` - Scheduler status
- `POST /api/v1/scheduler/trigger` - Manual collection trigger

## Operational Procedures

### Starting Services

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f mlb-data-service
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (destructive)
docker-compose down -v
```

### Scaling Services

```bash
# Scale specific service
docker-compose up -d --scale mlb-data-service=2

# Update resource limits in docker-compose.yml
```

### Backup Procedures

1. **Database Backup**:
```bash
docker-compose exec postgres pg_dump -U mlb_user mlb_data > backup_$(date +%Y%m%d).sql
```

2. **Configuration Backup**:
```bash
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/ .env.production
```

3. **Log Archive**:
```bash
tar -czf logs_backup_$(date +%Y%m%d).tar.gz logs/
```

### Maintenance

#### Regular Tasks

1. **Daily**:
   - Check service health via Grafana
   - Review error logs
   - Verify data collection success

2. **Weekly**:
   - Review and acknowledge alerts
   - Check disk space usage
   - Update security patches

3. **Monthly**:
   - Review performance metrics
   - Optimize database queries
   - Update monitoring thresholds

#### Log Rotation

Logs are automatically rotated when they exceed 100MB:
- Keeps 5 backup files
- Compresses old logs
- Configurable in `config/production.yaml`

## Troubleshooting

### Common Issues

1. **Service Won't Start**:
   ```bash
   # Check logs
   docker-compose logs [service_name]
   
   # Verify configuration
   python3 tests/validate_production_deployment.py
   ```

2. **Database Connection Issues**:
   ```bash
   # Test database connectivity
   docker-compose exec postgres pg_isready -U mlb_user
   
   # Check connection pool
   curl http://localhost:8101/api/v1/status
   ```

3. **Monitoring Not Working**:
   ```bash
   # Check Prometheus targets
   curl http://localhost:9090/api/v1/targets
   
   # Verify service metrics
   curl http://localhost:8101/metrics
   ```

4. **High Memory Usage**:
   ```bash
   # Check resource usage
   docker stats
   
   # Review memory limits in docker-compose.yml
   ```

### Performance Tuning

1. **Database**:
   - Adjust `connection_pool_size` in production.yaml
   - Monitor query performance via logs
   - Consider read replicas for high load

2. **Application**:
   - Tune worker processes
   - Optimize API rate limits
   - Enable response caching

3. **Monitoring**:
   - Adjust scrape intervals
   - Optimize retention periods
   - Configure recording rules

## Security Considerations

### Production Checklist

- [ ] Change default passwords
- [ ] Configure SSL/TLS certificates
- [ ] Set up proper firewall rules
- [ ] Enable audit logging
- [ ] Configure backup encryption
- [ ] Set up VPN access
- [ ] Review CORS settings
- [ ] Enable rate limiting
- [ ] Configure alert notification channels
- [ ] Set up log retention policies

### Network Security

1. **Firewall Configuration**:
   - Only expose necessary ports
   - Use reverse proxy for external access
   - Configure internal network isolation

2. **Authentication**:
   - Change default Grafana password
   - Set up proper user roles
   - Configure API authentication

3. **Data Protection**:
   - Encrypt database connections
   - Secure backup storage
   - Regular security updates

## Support & Maintenance

### Monitoring Health

Use the integrated monitoring to track:
- Service availability (target: 99.9% uptime)
- Response time (target: < 1s 95th percentile)
- Error rate (target: < 1%)
- Data freshness (target: updated daily)

### Performance Baselines

- **Request Rate**: 100-500 requests/minute
- **Memory Usage**: 512MB-2GB per service
- **CPU Usage**: 10-50% under normal load
- **Database Connections**: 5-15 active connections

### Contact Information

For production issues:
1. Check Grafana dashboards first
2. Review application logs
3. Check system resource usage
4. Escalate to operations team if needed

---

**Note**: This deployment guide assumes a production environment. For development or testing, use simplified configurations and remove security restrictions as appropriate.
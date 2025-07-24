#!/bin/bash

echo "🏢 StatEdge MLB Analytics Platform - Production Deployment"
echo "==========================================================="

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose -f docker-compose.statedge.yml down

# Remove old images (optional)
echo "🧹 Cleaning up old images..."
docker image prune -f

# Build and start services
echo "🚀 Building and starting StatEdge services..."
docker-compose -f docker-compose.statedge.yml up --build -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be healthy..."
sleep 15

# Check service status
echo "📋 Service Status:"
docker-compose -f docker-compose.statedge.yml ps

# Test health endpoints
echo ""
echo "🧪 Testing StatEdge endpoints:"
echo "📊 Dashboard: http://localhost:8081/monitoring"
echo "🏥 Health: http://localhost:8081/health"
echo "📡 API: http://localhost:8081/api/status"

# Test if services respond
sleep 5
echo ""
echo "🔍 Health Check Results:"
curl -s http://localhost:8081/health | head -3 || echo "❌ Health check failed"

echo ""
echo "✅ StatEdge deployment complete!"
echo "🌐 Access your dashboard at: http://localhost:8081/monitoring"
echo "🏢 Company: StatEdge (statedge.app)"
echo "📈 Service: MLB Analytics Platform"
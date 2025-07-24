#!/bin/bash

echo "🔧 StatEdge Docker & Database Recovery Script"
echo "=============================================="

# Function to check if Docker is responsive
check_docker() {
    timeout 10 docker version >/dev/null 2>&1
    return $?
}

# Function to restart Docker (if possible)
restart_docker() {
    echo "🔄 Attempting to restart Docker..."
    
    # Try different restart methods
    if command -v systemctl >/dev/null 2>&1; then
        echo "Using systemctl to restart Docker..."
        sudo systemctl restart docker 2>/dev/null || echo "❌ systemctl restart failed"
    fi
    
    if command -v service >/dev/null 2>&1; then
        echo "Using service to restart Docker..."
        sudo service docker restart 2>/dev/null || echo "❌ service restart failed"
    fi
    
    sleep 5
}

# Function to clean up Docker
cleanup_docker() {
    echo "🧹 Cleaning up Docker..."
    
    # Kill any hanging processes
    sudo pkill -f docker 2>/dev/null || true
    
    # Remove Docker lock files
    sudo rm -f /var/lib/docker/engine-id 2>/dev/null || true
    sudo rm -f /var/run/docker.sock 2>/dev/null || true
    sudo rm -f /var/run/docker.pid 2>/dev/null || true
    
    echo "✅ Docker cleanup complete"
}

# Function to start StatEdge database containers
start_statedge_containers() {
    echo "🚀 Starting StatEdge containers..."
    
    # Change to project directory
    cd /home/jeffreyconboy/github-repos/mlb-data-service
    
    # Start containers
    docker-compose -f docker-compose.statedge.yml up -d
    
    if [ $? -eq 0 ]; then
        echo "✅ Containers started successfully"
        
        # Wait for database to be ready
        echo "⏳ Waiting for database to be ready..."
        sleep 15
        
        # Test database connection
        docker exec statedge-postgres pg_isready -U statedge_user -d mlb_data
        
        if [ $? -eq 0 ]; then
            echo "✅ Database is ready!"
            echo "🎯 Connection details:"
            echo "   Host: localhost"
            echo "   Port: 5439"
            echo "   Database: mlb_data"
            echo "   Username: statedge_user"
            echo "   Password: statedge_secure_2024"
        else
            echo "❌ Database not responding"
        fi
    else
        echo "❌ Failed to start containers"
        return 1
    fi
}

# Main execution
echo "📋 Checking Docker status..."

if check_docker; then
    echo "✅ Docker is responsive"
    start_statedge_containers
else
    echo "❌ Docker is not responsive"
    echo "🔧 Attempting to fix Docker..."
    
    cleanup_docker
    restart_docker
    
    # Wait and check again
    sleep 10
    
    if check_docker; then
        echo "✅ Docker is now working"
        start_statedge_containers
    else
        echo "❌ Docker is still not working"
        echo "📋 Manual steps needed:"
        echo "   1. Restart Docker Desktop (or Docker service)"
        echo "   2. Run: docker system prune -f"
        echo "   3. Run this script again"
        exit 1
    fi
fi

echo ""
echo "🎯 Next steps:"
echo "   1. Test DBeaver connection to localhost:5439"
echo "   2. Run: curl http://localhost:8090/api/data-collection/status"
echo "   3. Check dashboard: http://localhost:8090/monitoring"
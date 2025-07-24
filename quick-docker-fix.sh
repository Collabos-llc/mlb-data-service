#!/bin/bash

echo "🔧 Quick Docker Desktop WSL Integration Fix"
echo "=========================================="

# Check if Docker Desktop is installed but not integrated
if [ -f "/mnt/c/Program Files/Docker/Docker/Docker Desktop.exe" ]; then
    echo "✅ Docker Desktop found on Windows"
    echo "🔧 Attempting to fix WSL integration..."
    
    # Try to start Docker Desktop
    echo "1. Starting Docker Desktop..."
    "/mnt/c/Program Files/Docker/Docker/Docker Desktop.exe" &
    
    echo "2. Waiting for Docker Desktop to start..."
    sleep 10
    
    # Check if docker command works now
    if docker --version >/dev/null 2>&1; then
        echo "✅ Docker Desktop WSL integration working!"
        docker --version
        echo ""
        echo "🎯 Now run: ./fix-docker-and-database.sh"
    else
        echo "❌ Still need manual fix"
        echo ""
        echo "📋 Manual steps:"
        echo "1. Open Docker Desktop"
        echo "2. Go to Settings > Resources > WSL Integration"
        echo "3. Enable integration with your WSL distro"
        echo "4. Click 'Apply & Restart'"
        echo "5. Restart this terminal"
    fi
else
    echo "❌ Docker Desktop not found"
    echo "📋 Need to reinstall Docker Desktop:"
    echo "1. Download: https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe"
    echo "2. Run as Administrator"
    echo "3. Enable WSL 2 integration during install"
fi
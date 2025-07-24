#!/bin/bash

echo "🐳 Docker Installation & Recovery Script for WSL"
echo "================================================"

# Function to check if Docker is installed
check_docker() {
    if command -v docker >/dev/null 2>&1; then
        echo "✅ Docker command found"
        docker --version
        return 0
    else
        echo "❌ Docker command not found"
        return 1
    fi
}

# Function to install Docker in WSL
install_docker_wsl() {
    echo "🔧 Installing Docker directly in WSL..."
    
    # Update package index
    sudo apt-get update
    
    # Install prerequisites
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    
    # Add Docker's official GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    
    # Set up the repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Update package index again
    sudo apt-get update
    
    # Install Docker Engine
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    
    # Add user to docker group
    sudo usermod -aG docker $USER
    
    # Start Docker service
    sudo service docker start
    
    echo "✅ Docker installed in WSL"
}

# Function to install Docker Compose standalone
install_docker_compose() {
    echo "🔧 Installing Docker Compose..."
    
    # Download docker-compose
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    
    # Make it executable
    sudo chmod +x /usr/local/bin/docker-compose
    
    # Create symlink
    sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
    
    echo "✅ Docker Compose installed"
}

# Function to start Docker service
start_docker() {
    echo "🚀 Starting Docker service..."
    
    # Start Docker daemon
    sudo service docker start
    
    # Enable Docker to start on boot
    sudo systemctl enable docker 2>/dev/null || true
    
    # Test Docker
    if sudo docker run hello-world >/dev/null 2>&1; then
        echo "✅ Docker is working!"
        return 0
    else
        echo "❌ Docker test failed"
        return 1
    fi
}

# Function to create Docker daemon config for WSL
configure_docker_wsl() {
    echo "🔧 Configuring Docker for WSL..."
    
    # Create Docker daemon config
    sudo mkdir -p /etc/docker
    
    cat << EOF | sudo tee /etc/docker/daemon.json
{
    "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2375"],
    "iptables": false,
    "bridge": "none"
}
EOF
    
    # Restart Docker
    sudo service docker restart
    
    echo "✅ Docker configured for WSL"
}

# Main execution
echo "📋 Checking current Docker status..."

if check_docker; then
    echo "🔧 Docker found but may not be working. Testing..."
    
    if docker ps >/dev/null 2>&1; then
        echo "✅ Docker is working!"
    else
        echo "❌ Docker daemon not running. Starting..."
        start_docker
    fi
else
    echo "❌ Docker not found. Installing..."
    
    # Ask user preference
    echo "📋 Installation options:"
    echo "1. Install Docker directly in WSL (recommended)"
    echo "2. Manual Docker Desktop reinstall instructions"
    echo ""
    read -p "Choose option (1 or 2): " choice
    
    case $choice in
        1)
            install_docker_wsl
            install_docker_compose
            configure_docker_wsl
            start_docker
            ;;
        2)
            echo "📋 To reinstall Docker Desktop:"
            echo "1. Download from: https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe"
            echo "2. Run installer as Administrator"
            echo "3. Enable WSL 2 integration"
            echo "4. Restart your computer"
            echo "5. Run this script again"
            exit 0
            ;;
        *)
            echo "❌ Invalid choice"
            exit 1
            ;;
    esac
fi

# Test final installation
echo ""
echo "🧪 Final Docker test..."

if docker --version && docker-compose --version; then
    echo "✅ Docker installation complete!"
    echo ""
    echo "🎯 Next steps:"
    echo "1. You may need to restart your terminal/WSL session"
    echo "2. Run: ./fix-docker-and-database.sh"
    echo "3. Test with: docker run hello-world"
    echo ""
    echo "🔧 If you get permission errors, run:"
    echo "   sudo usermod -aG docker \$USER"
    echo "   Then restart your terminal"
else
    echo "❌ Docker installation may have issues"
    echo "Try restarting your terminal and running: docker --version"
fi
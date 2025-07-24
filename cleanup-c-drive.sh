#!/bin/bash

echo "🧹 C: Drive Space Analysis & Cleanup Script"
echo "============================================"

echo "📊 Current C: Drive Status:"
df -h /mnt/c 2>/dev/null || echo "Cannot access C: drive from WSL"

echo ""
echo "🔍 Finding largest directories on C: drive..."
echo "This may take a few minutes..."

# Function to check common space-hogging locations
check_space_hogs() {
    echo ""
    echo "📋 Common Space-Consuming Locations:"
    echo "===================================="
    
    # Windows Update files
    if [ -d "/mnt/c/Windows/SoftwareDistribution" ]; then
        size=$(du -sh "/mnt/c/Windows/SoftwareDistribution" 2>/dev/null | cut -f1)
        echo "💾 Windows Updates: $size"
    fi
    
    # Temp files
    if [ -d "/mnt/c/Windows/Temp" ]; then
        size=$(du -sh "/mnt/c/Windows/Temp" 2>/dev/null | cut -f1)
        echo "🗂️ Windows Temp: $size"
    fi
    
    # User temp files
    if [ -d "/mnt/c/Users" ]; then
        for user_dir in /mnt/c/Users/*/AppData/Local/Temp; do
            if [ -d "$user_dir" ]; then
                size=$(du -sh "$user_dir" 2>/dev/null | cut -f1)
                username=$(echo "$user_dir" | cut -d'/' -f5)
                echo "👤 $username Temp: $size"
            fi
        done
    fi
    
    # Recycle Bin
    if [ -d "/mnt/c/\$Recycle.Bin" ]; then
        size=$(du -sh "/mnt/c/\$Recycle.Bin" 2>/dev/null | cut -f1)
        echo "🗑️ Recycle Bin: $size"
    fi
    
    # Docker data
    if [ -d "/mnt/c/ProgramData/Docker" ]; then
        size=$(du -sh "/mnt/c/ProgramData/Docker" 2>/dev/null | cut -f1)
        echo "🐳 Docker Data: $size"
    fi
    
    # WSL distributions
    if [ -d "/mnt/c/Users" ]; then
        for user_dir in /mnt/c/Users/*/AppData/Local/Packages/*/LocalState/rootfs; do
            if [ -d "$user_dir" ]; then
                size=$(du -sh "$user_dir" 2>/dev/null | cut -f1)
                distro=$(echo "$user_dir" | cut -d'/' -f7)
                echo "🐧 WSL ($distro): $size"
            fi
        done
    fi
    
    # Program Files
    if [ -d "/mnt/c/Program Files" ]; then
        size=$(du -sh "/mnt/c/Program Files" 2>/dev/null | cut -f1)
        echo "📁 Program Files: $size"
    fi
    
    # Downloads folder
    if [ -d "/mnt/c/Users" ]; then
        for downloads_dir in /mnt/c/Users/*/Downloads; do
            if [ -d "$downloads_dir" ]; then
                size=$(du -sh "$downloads_dir" 2>/dev/null | cut -f1)
                username=$(echo "$downloads_dir" | cut -d'/' -f5)
                echo "⬇️ $username Downloads: $size"
            fi
        done
    fi
    
    # Desktop files
    if [ -d "/mnt/c/Users" ]; then
        for desktop_dir in /mnt/c/Users/*/Desktop; do
            if [ -d "$desktop_dir" ]; then
                size=$(du -sh "$desktop_dir" 2>/dev/null | cut -f1)
                username=$(echo "$desktop_dir" | cut -d'/' -f5)
                echo "🖥️ $username Desktop: $size"
            fi
        done
    fi
}

# Function to suggest cleanup commands
suggest_cleanup() {
    echo ""
    echo "🧹 RECOMMENDED CLEANUP ACTIONS:"
    echo "==============================="
    echo ""
    echo "🔧 IMMEDIATE - Run these Windows commands as Administrator:"
    echo "  1. Disk Cleanup:"
    echo "     cleanmgr /d C:"
    echo ""
    echo "  2. Clear Windows Update cache:"
    echo "     net stop wuauserv"
    echo "     rd /s /q C:\Windows\SoftwareDistribution"
    echo "     net start wuauserv"
    echo ""
    echo "  3. Clear temp files:"
    echo "     del /s /q C:\Windows\Temp\*"
    echo "     del /s /q %TEMP%\*"
    echo ""
    echo "  4. Empty Recycle Bin (from Windows):"
    echo "     Right-click Recycle Bin → Empty"
    echo ""
    echo "🐳 DOCKER-SPECIFIC:"
    echo "  5. Clean Docker data (if Docker is working):"
    echo "     docker system prune -a --volumes"
    echo ""
    echo "📊 ANALYSIS TOOLS:"
    echo "  6. Use WinDirStat or TreeSize to find large files:"
    echo "     Download: https://windirstat.net/"
    echo ""
    echo "⚠️ CRITICAL: Need at least 5GB free for Docker to work properly!"
}

# Function to create PowerShell cleanup script
create_powershell_cleanup() {
    cleanup_script="/mnt/c/temp/cleanup-c-drive.ps1"
    
    echo "📝 Creating PowerShell cleanup script..."
    
    cat > "$cleanup_script" << 'EOF'
# StatEdge C: Drive Cleanup Script
# Run as Administrator

Write-Host "🧹 StatEdge C: Drive Cleanup" -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

# Get initial free space
$initialFree = (Get-WmiObject -Class Win32_LogicalDisk -Filter "DeviceID='C:'").FreeSpace / 1GB
Write-Host "📊 Initial free space: $([math]::Round($initialFree, 2)) GB"

# Clean temp files
Write-Host "🗂️ Cleaning temporary files..."
Get-ChildItem -Path "C:\Windows\Temp" -Recurse -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path "$env:TEMP" -Recurse -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Clean Windows Update cache
Write-Host "💾 Cleaning Windows Update cache..."
Stop-Service -Name wuauserv -Force -ErrorAction SilentlyContinue
if (Test-Path "C:\Windows\SoftwareDistribution\Download") {
    Remove-Item "C:\Windows\SoftwareDistribution\Download\*" -Recurse -Force -ErrorAction SilentlyContinue
}
Start-Service -Name wuauserv -ErrorAction SilentlyContinue

# Clean browser caches
Write-Host "🌐 Cleaning browser caches..."
$chromeCache = "$env:LOCALAPPDATA\Google\Chrome\User Data\Default\Cache"
if (Test-Path $chromeCache) {
    Remove-Item "$chromeCache\*" -Recurse -Force -ErrorAction SilentlyContinue
}

# Clean Docker if present
Write-Host "🐳 Cleaning Docker data..."
if (Get-Command docker -ErrorAction SilentlyContinue) {
    docker system prune -a -f --volumes
}

# Run Disk Cleanup utility
Write-Host "🔧 Running Disk Cleanup..."
Start-Process cleanmgr -ArgumentList "/d C: /verylowdisk" -Wait

# Get final free space
$finalFree = (Get-WmiObject -Class Win32_LogicalDisk -Filter "DeviceID='C:'").FreeSpace / 1GB
$cleaned = $finalFree - $initialFree

Write-Host "✅ Cleanup complete!" -ForegroundColor Green
Write-Host "📊 Final free space: $([math]::Round($finalFree, 2)) GB" -ForegroundColor Green
Write-Host "🧹 Space cleaned: $([math]::Round($cleaned, 2)) GB" -ForegroundColor Green

if ($finalFree -gt 5) {
    Write-Host "🎯 Ready for Docker installation!" -ForegroundColor Green
} else {
    Write-Host "⚠️ Still need more space for Docker (recommend 5GB+)" -ForegroundColor Yellow
}
EOF

    echo "✅ PowerShell cleanup script created: C:\\temp\\cleanup-c-drive.ps1"
    echo ""
    echo "🚀 To run it:"
    echo "  1. Open PowerShell as Administrator"
    echo "  2. Run: C:\\temp\\cleanup-c-drive.ps1"
}

# Main execution
check_space_hogs
suggest_cleanup
create_powershell_cleanup

echo ""
echo "🎯 NEXT STEPS:"
echo "=============="
echo "1. 🧹 Run the cleanup actions above"
echo "2. 🎯 Free up at least 5GB of space"
echo "3. 🐳 Reinstall Docker Desktop"
echo "4. 📊 Use your E: drive database in the meantime"
echo ""
echo "💡 Your StatEdge database is ready on E: drive!"
echo "   Path: E:\\StatEdge\\statedge_mlb_full.db"
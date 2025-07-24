#!/bin/bash

echo "🏢 StatEdge MLB Analytics Platform - Slack Integration Deployment"
echo "================================================================="

# Your Slack webhook URL
SLACK_WEBHOOK_URL="https://hooks.slack.com/services/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"

# Find available port
for port in 8090 8091 8092 8093 8094 8095; do
    if ! netstat -tln 2>/dev/null | grep ":$port " > /dev/null; then
        AVAILABLE_PORT=$port
        break
    fi
done

if [ -z "$AVAILABLE_PORT" ]; then
    echo "❌ No available ports found"
    exit 1
fi

echo "🔧 Using port: $AVAILABLE_PORT"

# Kill any existing StatEdge processes
pkill -f quick_statedge 2>/dev/null || true
sleep 2

# Activate virtual environment if it exists, create if not
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install flask flask-cors psutil requests
else
    source venv/bin/activate
fi

# Set environment variables
export SLACK_WEBHOOK_URL="$SLACK_WEBHOOK_URL"
export PORT=$AVAILABLE_PORT

echo "🚀 Starting StatEdge with Slack integration..."
echo "📍 Dashboard: http://localhost:$AVAILABLE_PORT/monitoring"
echo "🔍 Health: http://localhost:$AVAILABLE_PORT/health"
echo "📊 API: http://localhost:$AVAILABLE_PORT/api/status"
echo "📱 Slack: Enabled"
echo ""

# Start StatEdge in background
nohup python3 quick_statedge.py > statedge_$AVAILABLE_PORT.log 2>&1 &
STATEDGE_PID=$!

# Wait for startup
echo "⏳ Waiting for StatEdge to start..."
sleep 5

# Test if it's running
if curl -s http://localhost:$AVAILABLE_PORT/health > /dev/null; then
    echo "✅ StatEdge is running successfully!"
    
    # Test Slack integration
    echo "🧪 Testing Slack integration..."
    
    # Send startup notification
    curl -s -X POST http://localhost:$AVAILABLE_PORT/api/slack/startup > /dev/null
    
    # Send test notification  
    curl -s -X POST http://localhost:$AVAILABLE_PORT/api/slack/test > /dev/null
    
    echo "📱 Slack notifications sent!"
    echo ""
    echo "🎯 StatEdge is ready with Slack integration!"
    echo "📋 Process ID: $STATEDGE_PID"
    echo "📄 Logs: statedge_$AVAILABLE_PORT.log"
    echo ""
    echo "📊 Available endpoints:"
    echo "   • Dashboard: http://localhost:$AVAILABLE_PORT/monitoring"
    echo "   • Health: http://localhost:$AVAILABLE_PORT/health"  
    echo "   • API Status: http://localhost:$AVAILABLE_PORT/api/status"
    echo "   • Slack Test: curl -X POST http://localhost:$AVAILABLE_PORT/api/slack/test"
    echo "   • Daily Report: curl -X POST http://localhost:$AVAILABLE_PORT/api/slack/report"
    echo ""
    echo "🛑 To stop: kill $STATEDGE_PID"
    
else
    echo "❌ StatEdge failed to start"
    echo "📄 Check logs: tail -f statedge_$AVAILABLE_PORT.log"
    exit 1
fi
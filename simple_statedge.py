#!/usr/bin/env python3
"""
Simple StatEdge Dashboard - Minimal Working Version
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
import os
import psutil
from datetime import datetime

# Initialize Flask app with correct paths
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')
CORS(app)

@app.route('/')
def index():
    return jsonify({
        "company": "StatEdge",
        "service": "MLB Analytics Platform",
        "status": "operational",
        "url": "https://statedge.app"
    })

@app.route('/health')
def health():
    """Simple health check"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "company": "StatEdge",
        "service": "MLB Analytics Platform"
    })

@app.route('/monitoring')
def monitoring_dashboard():
    """Serve the StatEdge monitoring dashboard"""
    try:
        return render_template('monitoring/dashboard.html')
    except Exception as e:
        return f"""
        <html>
        <head><title>StatEdge - Error</title></head>
        <body>
            <h1>StatEdge MLB Analytics</h1>
            <p>Dashboard loading issue: {str(e)}</p>
            <p>Template path issue - checking template files...</p>
            <a href="/health">Health Check</a> | 
            <a href="/api/status">API Status</a>
        </body>
        </html>
        """

@app.route('/api/status')
def api_status():
    """Simple API status endpoint"""
    try:
        # Basic system metrics
        cpu = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return jsonify({
            "company": "StatEdge",
            "service": "MLB Analytics Platform", 
            "status": "operational",
            "system_metrics": {
                "cpu_percent": cpu,
                "memory_percent": memory.percent,
                "disk_percent": (disk.used / disk.total) * 100
            },
            "timestamp": datetime.now().isoformat(),
            "message": "StatEdge monitoring is operational"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "message": "StatEdge monitoring has issues"
        })

if __name__ == '__main__':
    print("🏢 Starting StatEdge MLB Analytics (Simple Version)...")
    print("📍 Dashboard: http://localhost:8200/monitoring")
    print("🔍 Health: http://localhost:8200/health") 
    print("📊 API: http://localhost:8200/api/status")
    
    app.run(host='0.0.0.0', port=8200, debug=True)
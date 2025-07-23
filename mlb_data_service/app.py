#!/usr/bin/env python3
"""
MLB Data Service - Containerized Foundation
==========================================

Main Flask application for MLB Data Service microservice.
Provides REST API endpoints for external data collection and serving.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import external API manager
from external_apis import ExternalAPIManager

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize external API manager
api_manager = ExternalAPIManager()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/mlb_data_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# In-memory storage for demo (will be replaced with database)
data_store = {
    'players': [],
    'games': [],
    'statcast': [],
    'collection_status': {
        'last_updated': None,
        'collections_today': 0,
        'status': 'ready'
    }
}

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for container orchestration"""
    return jsonify({
        'status': 'healthy',
        'service': 'MLB Data Service',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }), 200

@app.route('/api/v1/status', methods=['GET'])
def get_service_status():
    """Get current service status and statistics"""
    return jsonify({
        'service_name': 'MLB Data Service',
        'status': data_store['collection_status']['status'],
        'last_updated': data_store['collection_status']['last_updated'],
        'collections_today': data_store['collection_status']['collections_today'],
        'data_counts': {
            'players': len(data_store['players']),
            'games': len(data_store['games']),
            'statcast_records': len(data_store['statcast'])
        },
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/api/v1/players', methods=['GET'])
def get_players():
    """Get all collected player data"""
    return jsonify({
        'players': data_store['players'],
        'count': len(data_store['players']),
        'last_updated': data_store['collection_status']['last_updated']
    }), 200

@app.route('/api/v1/games/today', methods=['GET'])
def get_todays_games():
    """Get today's MLB games"""
    return jsonify({
        'games': data_store['games'],
        'count': len(data_store['games']),
        'date': datetime.now().strftime('%Y-%m-%d')
    }), 200

@app.route('/api/v1/statcast', methods=['GET'])
def get_statcast_data():
    """Get Statcast data with optional player filter"""
    player_name = request.args.get('player_name')
    
    if player_name:
        filtered_data = [
            record for record in data_store['statcast'] 
            if record.get('player_name', '').lower() == player_name.lower()
        ]
        return jsonify({
            'statcast_data': filtered_data,
            'count': len(filtered_data),
            'player_filter': player_name
        }), 200
    
    return jsonify({
        'statcast_data': data_store['statcast'][:100],  # Limit for demo
        'total_count': len(data_store['statcast']),
        'showing': min(100, len(data_store['statcast']))
    }), 200

@app.route('/api/v1/collect/players', methods=['POST'])
def collect_players():
    """Trigger player data collection from external APIs"""
    try:
        logger.info("Starting real player data collection...")
        
        # Get limit from request parameters
        limit = request.json.get('limit', 25) if request.json else 25
        
        # Collect real player data using external APIs
        collected_players = api_manager.collect_active_players(limit=limit)
        
        # Store collected data
        data_store['players'] = collected_players
        data_store['collection_status']['last_updated'] = datetime.now().isoformat()
        data_store['collection_status']['collections_today'] += 1
        data_store['collection_status']['status'] = 'active'
        
        logger.info(f"Collected {len(collected_players)} players successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Player data collection completed',
            'players_collected': len(collected_players),
            'data_source': collected_players[0].get('data_source', 'unknown') if collected_players else 'none',
            'collection_time': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Player collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Player collection failed: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/collect/games', methods=['POST'])
def collect_games():
    """Trigger today's games collection from MLB API"""
    try:
        logger.info("Starting real games data collection...")
        
        # Collect real games data using external APIs
        collected_games = api_manager.collect_todays_games()
        
        # Store collected data
        data_store['games'] = collected_games
        data_store['collection_status']['last_updated'] = datetime.now().isoformat()
        data_store['collection_status']['collections_today'] += 1
        
        logger.info(f"Collected {len(collected_games)} games successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Games data collection completed',
            'games_collected': len(collected_games),
            'data_source': collected_games[0].get('data_source', 'unknown') if collected_games else 'none',
            'collection_time': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Games collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Games collection failed: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/collect/statcast', methods=['POST'])
def collect_statcast():
    """Trigger Statcast data collection from PyBaseball"""
    try:
        logger.info("Starting real Statcast data collection...")
        
        # Get parameters from request
        days_back = request.json.get('days_back', 3) if request.json else 3
        limit = request.json.get('limit', 50) if request.json else 50
        
        # Collect real Statcast data using external APIs
        collected_statcast = api_manager.collect_recent_statcast(days_back=days_back, limit=limit)
        
        # Store collected data
        data_store['statcast'] = collected_statcast
        data_store['collection_status']['last_updated'] = datetime.now().isoformat()
        data_store['collection_status']['collections_today'] += 1
        
        logger.info(f"Collected {len(collected_statcast)} Statcast records successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Statcast data collection completed',
            'records_collected': len(collected_statcast),
            'days_back': days_back,
            'data_source': collected_statcast[0].get('data_source', 'unknown') if collected_statcast else 'none',
            'collection_time': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Statcast collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Statcast collection failed: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Endpoint not found',
        'message': 'The requested API endpoint does not exist',
        'timestamp': datetime.now().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred',
        'timestamp': datetime.now().isoformat()
    }), 500

if __name__ == '__main__':
    # Create logs directory if it doesn't exist
    os.makedirs('/app/logs', exist_ok=True)
    
    logger.info("Starting MLB Data Service...")
    logger.info("Service available at http://localhost:8001")
    
    # Run the Flask application
    app.run(
        host='0.0.0.0',
        port=8001,
        debug=os.getenv('FLASK_ENV') == 'development'
    )
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

# Import external API manager, database, and scheduler
from external_apis import ExternalAPIManager
from database import DatabaseManager
from scheduler import start_scheduler, get_scheduler

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize external API manager, database, and scheduler
api_manager = ExternalAPIManager()
db_manager = DatabaseManager()

# Global scheduler instance
scheduler = None

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

# Database storage - replaced in-memory storage
# All data operations now use PostgreSQL database

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for container orchestration"""
    # Test database connection as part of health check
    db_healthy = db_manager.test_connection()
    
    return jsonify({
        'status': 'healthy' if db_healthy else 'unhealthy',
        'service': 'MLB Data Service',
        'database': 'connected' if db_healthy else 'disconnected',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }), 200 if db_healthy else 503

@app.route('/api/v1/status', methods=['GET'])
def get_service_status():
    """Get current service status and statistics"""
    try:
        stats = db_manager.get_collection_stats()
        # Get scheduler status
        scheduler_status = get_scheduler().get_job_status() if scheduler else {'status': 'not_initialized'}
        
        return jsonify({
            'service_name': 'MLB Data Service',
            'status': 'active',
            'database_connected': db_manager.test_connection(),
            'scheduler_status': scheduler_status['status'],
            'data_counts': stats.get('data_counts', {}),
            'last_collections': stats.get('last_collections', {}),
            'next_scheduled_runs': scheduler_status.get('jobs', []),
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            'service_name': 'MLB Data Service',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/players', methods=['GET'])
def get_players():
    """Get all collected player data"""
    try:
        limit = request.args.get('limit', type=int)
        players = db_manager.get_players(limit=limit)
        return jsonify({
            'players': players,
            'count': len(players),
            'source': 'database'
        }), 200
    except Exception as e:
        logger.error(f"Failed to get players: {e}")
        return jsonify({
            'error': 'Failed to retrieve players',
            'message': str(e)
        }), 500

@app.route('/api/v1/games/today', methods=['GET'])
def get_todays_games():
    """Get today's MLB games"""
    try:
        games = db_manager.get_todays_games()
        return jsonify({
            'games': games,
            'count': len(games),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'database'
        }), 200
    except Exception as e:
        logger.error(f"Failed to get today's games: {e}")
        return jsonify({
            'error': 'Failed to retrieve games',
            'message': str(e)
        }), 500

@app.route('/api/v1/statcast', methods=['GET'])
def get_statcast_data():
    """Get Statcast data with optional player filter"""
    try:
        player_name = request.args.get('player_name')
        limit = request.args.get('limit', 100, type=int)
        
        statcast_data = db_manager.get_statcast_data(player_name=player_name, limit=limit)
        
        response = {
            'statcast_data': statcast_data,
            'count': len(statcast_data),
            'source': 'database'
        }
        
        if player_name:
            response['player_filter'] = player_name
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Failed to get Statcast data: {e}")
        return jsonify({
            'error': 'Failed to retrieve Statcast data',
            'message': str(e)
        }), 500

@app.route('/api/v1/collect/players', methods=['POST'])
def collect_players():
    """Trigger player data collection from external APIs"""
    try:
        logger.info("Starting real player data collection...")
        
        # Get limit from request parameters
        limit = request.json.get('limit', 25) if request.json else 25
        
        # Log collection start
        db_manager.log_collection_status('players', 'running')
        
        # Collect real player data using external APIs
        collected_players = api_manager.collect_active_players(limit=limit)
        
        # Store collected data in database
        stored_count = db_manager.store_players(collected_players)
        
        # Log collection completion
        db_manager.log_collection_status(
            'players', 'completed', 
            records_collected=stored_count,
            data_source=collected_players[0].get('data_source', 'unknown') if collected_players else 'none'
        )
        
        logger.info(f"Collected and stored {stored_count} players successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Player data collection completed',
            'players_collected': stored_count,
            'data_source': collected_players[0].get('data_source', 'unknown') if collected_players else 'none',
            'collection_time': datetime.now().isoformat(),
            'storage': 'database'
        }), 200
        
    except Exception as e:
        # Log collection failure
        db_manager.log_collection_status('players', 'failed', error_message=str(e))
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
        
        # Log collection start
        db_manager.log_collection_status('games', 'running')
        
        # Collect real games data using external APIs
        collected_games = api_manager.collect_todays_games()
        
        # Store collected data in database
        stored_count = db_manager.store_games(collected_games)
        
        # Log collection completion
        db_manager.log_collection_status(
            'games', 'completed',
            records_collected=stored_count,
            data_source=collected_games[0].get('data_source', 'unknown') if collected_games else 'none'
        )
        
        logger.info(f"Collected and stored {stored_count} games successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Games data collection completed',
            'games_collected': stored_count,
            'data_source': collected_games[0].get('data_source', 'unknown') if collected_games else 'none',
            'collection_time': datetime.now().isoformat(),
            'storage': 'database'
        }), 200
        
    except Exception as e:
        # Log collection failure
        db_manager.log_collection_status('games', 'failed', error_message=str(e))
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
        
        # Log collection start
        db_manager.log_collection_status('statcast', 'running')
        
        # Collect real Statcast data using external APIs
        collected_statcast = api_manager.collect_recent_statcast(days_back=days_back, limit=limit)
        
        # Store collected data in database
        stored_count = db_manager.store_statcast(collected_statcast)
        
        # Log collection completion
        db_manager.log_collection_status(
            'statcast', 'completed',
            records_collected=stored_count,
            data_source=collected_statcast[0].get('data_source', 'unknown') if collected_statcast else 'none'
        )
        
        logger.info(f"Collected and stored {stored_count} Statcast records successfully")
        
        return jsonify({
            'status': 'success',
            'message': 'Statcast data collection completed',
            'records_collected': stored_count,
            'days_back': days_back,
            'data_source': collected_statcast[0].get('data_source', 'unknown') if collected_statcast else 'none',
            'collection_time': datetime.now().isoformat(),
            'storage': 'database'
        }), 200
        
    except Exception as e:
        # Log collection failure
        db_manager.log_collection_status('statcast', 'failed', error_message=str(e))
        logger.error(f"Statcast collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Statcast collection failed: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/scheduler/status', methods=['GET'])
def get_scheduler_status():
    """Get detailed scheduler status"""
    try:
        scheduler_instance = get_scheduler()
        status = scheduler_instance.get_job_status()
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        return jsonify({
            'error': 'Failed to get scheduler status',
            'message': str(e)
        }), 500

@app.route('/api/v1/scheduler/trigger', methods=['POST'])
def trigger_daily_collection():
    """Manually trigger daily data collection"""
    try:
        logger.info("Manual daily collection triggered via API")
        scheduler_instance = get_scheduler()
        result = scheduler_instance.trigger_daily_collection()
        
        return jsonify({
            'status': 'triggered',
            'message': 'Daily collection started',
            'collection_result': result,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Failed to trigger daily collection: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to trigger collection: {str(e)}',
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
    
    # Start the scheduler
    try:
        global scheduler
        scheduler = start_scheduler("http://localhost:8001")
        logger.info("✅ Automated scheduler started - daily collection at 7 AM")
    except Exception as e:
        logger.error(f"❌ Failed to start scheduler: {e}")
        scheduler = None
    
    logger.info("Service available at http://localhost:8001")
    
    try:
        # Run the Flask application
        app.run(
            host='0.0.0.0',
            port=8001,
            debug=os.getenv('FLASK_ENV') == 'development'
        )
    finally:
        # Cleanup scheduler on shutdown
        if scheduler:
            logger.info("Shutting down scheduler...")
            scheduler.stop()
#!/usr/bin/env python3
"""
Enhanced MLB Data Service
========================

Flask application with comprehensive FanGraphs and Statcast data support.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import enhanced database manager
from enhanced_database import EnhancedDatabaseManager

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize enhanced database manager
db_manager = EnhancedDatabaseManager()

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

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for container orchestration"""
    try:
        # Test database connection
        db_connected = db_manager.test_connection()
        
        return jsonify({
            'status': 'healthy' if db_connected else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'service': 'mlb-data-service-enhanced',
            'version': '2.0.0',
            'database': 'connected' if db_connected else 'disconnected'
        }), 200 if db_connected else 503
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 503

@app.route('/api/v1/status', methods=['GET'])
def service_status():
    """Service status endpoint with comprehensive database statistics"""
    try:
        stats = db_manager.get_database_stats()
        
        return jsonify({
            'service': 'mlb-data-service-enhanced',
            'status': 'operational',
            'timestamp': datetime.now().isoformat(),
            'database_stats': stats,
            'capabilities': {
                'fangraphs_batting': '320+ metrics per player',
                'fangraphs_pitching': '390+ metrics per pitcher', 
                'statcast': '110+ fields per pitch',
                'comprehensive_analytics': True
            }
        })
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            'service': 'mlb-data-service-enhanced',
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500

@app.route('/api/v1/collect/fangraphs/batting', methods=['POST'])
def collect_fangraphs_batting():
    """Collect comprehensive FanGraphs batting data"""
    try:
        data = request.get_json() or {}
        season = data.get('season', datetime.now().year)
        min_pa = data.get('min_pa', 10)
        
        logger.info(f"Starting FanGraphs batting collection for {season}")
        
        count = db_manager.collect_and_store_fangraphs_batting(season=season, min_pa=min_pa)
        
        if count > 0:
            return jsonify({
                'status': 'success',
                'message': f'Collected {count} FanGraphs batting records for {season}',
                'season': season,
                'records_collected': count,
                'min_pa': min_pa,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'warning',
                'message': 'No FanGraphs batting data collected',
                'season': season,
                'timestamp': datetime.now().isoformat()
            }), 204
            
    except Exception as e:
        logger.error(f"FanGraphs batting collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': 'FanGraphs batting collection failed',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/collect/fangraphs/pitching', methods=['POST'])
def collect_fangraphs_pitching():
    """Collect comprehensive FanGraphs pitching data"""
    try:
        data = request.get_json() or {}
        season = data.get('season', datetime.now().year)
        min_ip = data.get('min_ip', 5)
        
        logger.info(f"Starting FanGraphs pitching collection for {season}")
        
        count = db_manager.collect_and_store_fangraphs_pitching(season=season, min_ip=min_ip)
        
        if count > 0:
            return jsonify({
                'status': 'success',
                'message': f'Collected {count} FanGraphs pitching records for {season}',
                'season': season,
                'records_collected': count,
                'min_ip': min_ip,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'warning',
                'message': 'No FanGraphs pitching data collected',
                'season': season,
                'timestamp': datetime.now().isoformat()
            }), 204
            
    except Exception as e:
        logger.error(f"FanGraphs pitching collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': 'FanGraphs pitching collection failed',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/collect/statcast', methods=['POST'])
def collect_statcast():
    """Collect comprehensive Statcast data"""
    try:
        data = request.get_json() or {}
        
        # Default to today if no dates provided
        start_date = data.get('start_date', datetime.now().strftime('%Y-%m-%d'))
        end_date = data.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        
        logger.info(f"Starting Statcast collection from {start_date} to {end_date}")
        
        count = db_manager.collect_and_store_statcast(start_date=start_date, end_date=end_date)
        
        if count > 0:
            return jsonify({
                'status': 'success',
                'message': f'Collected {count} Statcast records',
                'start_date': start_date,
                'end_date': end_date,
                'records_collected': count,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'warning',
                'message': 'No Statcast data collected',
                'start_date': start_date,
                'end_date': end_date,
                'timestamp': datetime.now().isoformat()
            }), 204
            
    except Exception as e:
        logger.error(f"Statcast collection failed: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Statcast collection failed',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/fangraphs/batting', methods=['GET'])
def get_fangraphs_batting():
    """Get FanGraphs batting data summary"""
    try:
        season = request.args.get('season', datetime.now().year, type=int)
        limit = request.args.get('limit', 20, type=int)
        
        data = db_manager.get_fangraphs_batting_summary(season=season, limit=limit)
        
        return jsonify({
            'status': 'success',
            'season': season,
            'count': len(data),
            'players': data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to get FanGraphs batting data: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve FanGraphs batting data',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/statcast', methods=['GET'])
def get_statcast():
    """Get Statcast data summary"""
    try:
        limit = request.args.get('limit', 20, type=int)
        
        data = db_manager.get_statcast_summary(limit=limit)
        
        return jsonify({
            'status': 'success',
            'count': len(data),
            'pitches': data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to get Statcast data: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve Statcast data',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/analytics/summary', methods=['GET'])
def analytics_summary():
    """Get comprehensive analytics summary"""
    try:
        stats = db_manager.get_database_stats()
        
        # Get sample data
        batting_sample = db_manager.get_fangraphs_batting_summary(limit=5)
        statcast_sample = db_manager.get_statcast_summary(limit=5)
        
        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'database_overview': stats,
            'sample_data': {
                'top_batters': batting_sample,
                'recent_pitches': statcast_sample
            },
            'capabilities': {
                'advanced_metrics': ['wOBA', 'wRC+', 'WAR', 'FIP', 'xFIP', 'SIERA'],
                'statcast_metrics': ['Exit Velocity', 'Launch Angle', 'Spin Rate', 'Expected wOBA'],
                'pitch_tracking': ['Location', 'Movement', 'Velocity', 'Spin Rate'],
                'total_fields': {
                    'batting': 320,
                    'pitching': 390,
                    'statcast': 110
                }
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to get analytics summary: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve analytics summary',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'status': 'error',
        'message': 'Endpoint not found',
        'timestamp': datetime.now().isoformat()
    }), 404

@app.route('/api/v1/player/profile', methods=['GET'])
def get_player_profile():
    """Get unified player profile combining all data sources"""
    try:
        # Get query parameters
        player_name = request.args.get('name')
        fangraphs_id = request.args.get('fangraphs_id', type=int)
        mlb_id = request.args.get('mlb_id', type=int)
        
        if not any([player_name, fangraphs_id, mlb_id]):
            return jsonify({
                'status': 'error',
                'message': 'Must provide name, fangraphs_id, or mlb_id parameter',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        profile = db_manager.get_unified_player_profile(
            player_name=player_name,
            fangraphs_id=fangraphs_id,
            mlb_id=mlb_id
        )
        
        if profile:
            return jsonify({
                'status': 'success',
                'player': profile,
                'data_sources': {
                    'player_lookup': True,
                    'fangraphs_2025': profile.get('plate_appearances') is not None,
                    'statcast': profile.get('statcast_abs', 0) > 0
                },
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'not_found',
                'message': 'Player not found',
                'timestamp': datetime.now().isoformat()
            }), 404
            
    except Exception as e:
        logger.error(f"Failed to get player profile: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve player profile',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/player/search', methods=['GET'])
def search_players():
    """Search for players across all systems"""
    try:
        query = request.args.get('q', '').strip()
        limit = request.args.get('limit', 10, type=int)
        
        if not query:
            return jsonify({
                'status': 'error',
                'message': 'Query parameter "q" is required',
                'timestamp': datetime.now().isoformat()
            }), 400
            
        if len(query) < 2:
            return jsonify({
                'status': 'error',
                'message': 'Query must be at least 2 characters',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        results = db_manager.search_players(query=query, limit=limit)
        
        return jsonify({
            'status': 'success',
            'query': query,
            'count': len(results),
            'players': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to search players: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to search players',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/v1/player/ids', methods=['GET'])
def get_player_id_mappings():
    """Get ID mappings for a specific player"""
    try:
        player_name = request.args.get('name')
        fangraphs_id = request.args.get('fangraphs_id', type=int)
        mlb_id = request.args.get('mlb_id', type=int)
        
        if not any([player_name, fangraphs_id, mlb_id]):
            return jsonify({
                'status': 'error',
                'message': 'Must provide name, fangraphs_id, or mlb_id parameter',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        profile = db_manager.get_unified_player_profile(
            player_name=player_name,
            fangraphs_id=fangraphs_id,
            mlb_id=mlb_id
        )
        
        if profile:
            id_mappings = {
                'full_name': profile.get('full_name'),
                'identifiers': {
                    'fangraphs_id': profile.get('key_fangraphs'),
                    'mlb_id': profile.get('key_mlbam'),
                    'bbref_id': profile.get('key_bbref'),
                    'retro_id': profile.get('key_retro')
                },
                'career_span': {
                    'first_year': profile.get('mlb_played_first'),
                    'last_year': profile.get('mlb_played_last')
                }
            }
            
            return jsonify({
                'status': 'success',
                'player_ids': id_mappings,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'not_found',
                'message': 'Player not found',
                'timestamp': datetime.now().isoformat()
            }), 404
            
    except Exception as e:
        logger.error(f"Failed to get player ID mappings: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve player ID mappings',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'status': 'error',
        'message': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

if __name__ == '__main__':
    logger.info("Starting Enhanced MLB Data Service...")
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 8001)),
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    )
#!/usr/bin/env python3
"""
Alert Management System
=======================

Comprehensive alert management with threshold evaluation, multi-channel notifications,
alert lifecycle management, and intelligent failure detection for MLB data service.
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
import sqlite3

# Optional imports with fallbacks
try:
    import smtplib
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Optional database import
try:
    from enhanced_database import EnhancedDatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    # Mock database manager for testing
    class EnhancedDatabaseManager:
        def __init__(self):
            pass

logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertState(Enum):
    """Alert lifecycle states"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

@dataclass
class Alert:
    """Individual alert data structure"""
    id: str
    name: str
    severity: AlertSeverity
    state: AlertState
    message: str
    source: str
    metric_value: Any
    threshold: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    notification_count: int = 0
    last_notification: Optional[datetime] = None

@dataclass
class NotificationChannel:
    """Notification channel configuration"""
    name: str
    type: str  # 'email', 'webhook', 'slack'
    config: Dict[str, Any]
    enabled: bool = True
    severity_filter: List[AlertSeverity] = field(default_factory=lambda: list(AlertSeverity))

class AlertManager:
    """Production-grade alert management system"""
    
    def __init__(self, database_manager: EnhancedDatabaseManager = None, config_file: str = None):
        """Initialize alert manager with configuration"""
        self.db_manager = database_manager or EnhancedDatabaseManager()
        
        # Load configuration
        self.config = self._load_config(config_file)
        
        # Alert storage (SQLite for persistence)
        self.alert_db_path = self.config.get('alert_db_path', '/tmp/mlb_alerts.db')
        self._init_alert_storage()
        
        # Active alerts tracking
        self.active_alerts: Dict[str, Alert] = {}
        self.notification_channels: List[NotificationChannel] = []
        
        # Alert suppression rules
        self.suppression_rules = self.config.get('suppression_rules', {})
        
        # Notification rate limiting
        self.rate_limits = self.config.get('rate_limits', {
            'critical': timedelta(minutes=5),
            'warning': timedelta(minutes=15),
            'info': timedelta(hours=1)
        })
        
        # Auto-recovery detection
        self.auto_recovery_enabled = self.config.get('auto_recovery', True)
        self.recovery_thresholds = self.config.get('recovery_thresholds', {})
        
        # Initialize notification channels
        self._setup_notification_channels()
        
        # Start background processing
        self._processing_active = True
        self._processing_thread = threading.Thread(target=self._background_processor, daemon=True)
        self._processing_thread.start()
        
        logger.info("AlertManager initialized")
    
    def _load_config(self, config_file: str = None) -> Dict[str, Any]:
        """Load alert manager configuration"""
        default_config = {
            'alert_db_path': '/tmp/mlb_alerts.db',
            'email': {
                'smtp_server': os.getenv('SMTP_SERVER', 'localhost'),
                'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                'username': os.getenv('SMTP_USERNAME'),
                'password': os.getenv('SMTP_PASSWORD'),
                'from_address': os.getenv('ALERT_FROM_EMAIL', 'alerts@mlb-service.com'),
                'to_addresses': os.getenv('ALERT_TO_EMAILS', '').split(',')
            },
            'webhooks': {
                'slack_webhook': os.getenv('SLACK_WEBHOOK_URL'),
                'custom_webhook': os.getenv('CUSTOM_WEBHOOK_URL')
            },
            'suppression_rules': {
                'duplicate_window': timedelta(minutes=10),
                'maintenance_mode': False
            },
            'auto_recovery': True,
            'recovery_thresholds': {
                'cpu_recovery': 60,  # CPU below 60% to recover
                'memory_recovery': 70,  # Memory below 70% to recover
                'response_time_recovery': 0.5  # Response time below 0.5s to recover
            }
        }
        
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                default_config.update(file_config)
            except Exception as e:
                logger.warning(f"Failed to load config file {config_file}: {e}")
        
        return default_config
    
    def _init_alert_storage(self):
        """Initialize SQLite database for alert persistence"""
        try:
            conn = sqlite3.connect(self.alert_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    state TEXT NOT NULL,
                    message TEXT NOT NULL,
                    source TEXT NOT NULL,
                    metric_value TEXT,
                    threshold TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    acknowledged_at TEXT,
                    resolved_at TEXT,
                    acknowledged_by TEXT,
                    metadata TEXT,
                    notification_count INTEGER DEFAULT 0,
                    last_notification TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_alert_state ON alerts(state)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_alert_created ON alerts(created_at)
            ''')
            
            conn.commit()
            conn.close()
            
            # Load active alerts from storage
            self._load_active_alerts()
            
        except Exception as e:
            logger.error(f"Failed to initialize alert storage: {e}")
    
    def _setup_notification_channels(self):
        """Setup notification channels based on configuration"""
        # Email channel
        if self.config['email']['username'] and self.config['email']['to_addresses']:
            email_channel = NotificationChannel(
                name='email',
                type='email',
                config=self.config['email'],
                severity_filter=[AlertSeverity.WARNING, AlertSeverity.CRITICAL]
            )
            self.notification_channels.append(email_channel)
        
        # Slack webhook
        if self.config['webhooks']['slack_webhook']:
            slack_channel = NotificationChannel(
                name='slack',
                type='webhook',
                config={
                    'url': self.config['webhooks']['slack_webhook'],
                    'format': 'slack'
                },
                severity_filter=[AlertSeverity.CRITICAL]
            )
            self.notification_channels.append(slack_channel)
        
        # Custom webhook
        if self.config['webhooks']['custom_webhook']:
            webhook_channel = NotificationChannel(
                name='custom_webhook',
                type='webhook',
                config={
                    'url': self.config['webhooks']['custom_webhook'],
                    'format': 'json'
                }
            )
            self.notification_channels.append(webhook_channel)
        
        logger.info(f"Initialized {len(self.notification_channels)} notification channels")
    
    def create_alert(self, name: str, severity: AlertSeverity, message: str, 
                    source: str, metric_value: Any = None, 
                    threshold: Dict[str, Any] = None, 
                    metadata: Dict[str, Any] = None) -> str:
        """Create a new alert"""
        alert_id = f"{source}_{name}_{int(time.time())}"
        
        # Check for suppression
        if self._is_suppressed(name, source):
            logger.info(f"Alert {name} from {source} suppressed")
            return alert_id
        
        alert = Alert(
            id=alert_id,
            name=name,
            severity=severity,
            state=AlertState.ACTIVE,
            message=message,
            source=source,
            metric_value=metric_value,
            threshold=threshold or {},
            created_at=datetime.now(),
            updated_at=datetime.now(),
            metadata=metadata or {}
        )
        
        # Store alert
        self.active_alerts[alert_id] = alert
        self._persist_alert(alert)
        
        # Send notifications
        self._send_notifications(alert)
        
        logger.warning(f"Alert created: {severity.value.upper()} - {name}: {message}")
        return alert_id
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str = "system") -> bool:
        """Acknowledge an active alert"""
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        if alert.state == AlertState.ACTIVE:
            alert.state = AlertState.ACKNOWLEDGED
            alert.acknowledged_at = datetime.now()
            alert.acknowledged_by = acknowledged_by
            alert.updated_at = datetime.now()
            
            self._persist_alert(alert)
            logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
            return True
        
        return False
    
    def resolve_alert(self, alert_id: str, resolution_message: str = None) -> bool:
        """Resolve an alert"""
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert.state = AlertState.RESOLVED
        alert.resolved_at = datetime.now()
        alert.updated_at = datetime.now()
        
        if resolution_message:
            alert.metadata['resolution_message'] = resolution_message
        
        self._persist_alert(alert)
        
        # Send resolution notification
        self._send_resolution_notification(alert)
        
        # Remove from active alerts
        del self.active_alerts[alert_id]
        
        logger.info(f"Alert {alert_id} resolved: {alert.name}")
        return True
    
    def auto_resolve_if_recovered(self, source: str, metric_name: str, current_value: float) -> List[str]:
        """Auto-resolve alerts if metrics have recovered"""
        if not self.auto_recovery_enabled:
            return []
        
        resolved_alerts = []
        
        for alert_id, alert in list(self.active_alerts.items()):
            if alert.source == source and metric_name in alert.name.lower():
                # Check if metric has recovered
                if self._has_metric_recovered(alert, current_value):
                    self.resolve_alert(alert_id, f"Auto-resolved: {metric_name} recovered to {current_value}")
                    resolved_alerts.append(alert_id)
        
        return resolved_alerts
    
    def _has_metric_recovered(self, alert: Alert, current_value: float) -> bool:
        """Check if a metric has recovered based on thresholds"""
        try:
            # CPU recovery
            if 'cpu' in alert.name.lower():
                return current_value < self.recovery_thresholds.get('cpu_recovery', 60)
            
            # Memory recovery
            if 'memory' in alert.name.lower():
                return current_value < self.recovery_thresholds.get('memory_recovery', 70)
            
            # Response time recovery
            if 'response' in alert.name.lower() or 'time' in alert.name.lower():
                return current_value < self.recovery_thresholds.get('response_time_recovery', 0.5)
            
            # Default: use warning threshold for recovery
            threshold = alert.threshold
            if isinstance(threshold, dict) and 'warning' in threshold:
                return current_value < threshold['warning']
            
        except Exception as e:
            logger.error(f"Error checking recovery for alert {alert.id}: {e}")
        
        return False
    
    def _is_suppressed(self, name: str, source: str) -> bool:
        """Check if alert should be suppressed"""
        # Maintenance mode suppression
        if self.suppression_rules.get('maintenance_mode', False):
            return True
        
        # Duplicate alert suppression
        duplicate_window = self.suppression_rules.get('duplicate_window', timedelta(minutes=10))
        cutoff_time = datetime.now() - duplicate_window
        
        for alert in self.active_alerts.values():
            if (alert.name == name and 
                alert.source == source and 
                alert.created_at > cutoff_time):
                return True
        
        return False
    
    def _send_notifications(self, alert: Alert):
        """Send notifications through configured channels"""
        for channel in self.notification_channels:
            if not channel.enabled:
                continue
            
            # Check severity filter
            if alert.severity not in channel.severity_filter:
                continue
            
            # Check rate limiting
            if not self._check_rate_limit(alert, channel):
                continue
            
            try:
                if channel.type == 'email':
                    self._send_email_notification(alert, channel)
                elif channel.type == 'webhook':
                    self._send_webhook_notification(alert, channel)
                
                # Update notification tracking
                alert.notification_count += 1
                alert.last_notification = datetime.now()
                
            except Exception as e:
                logger.error(f"Failed to send notification via {channel.name}: {e}")
    
    def _send_email_notification(self, alert: Alert, channel: NotificationChannel):
        """Send email notification"""
        if not EMAIL_AVAILABLE:
            logger.warning("Email libraries not available, skipping email notification")
            return
            
        config = channel.config
        
        msg = MimeMultipart()
        msg['From'] = config['from_address']
        msg['To'] = ', '.join(config['to_addresses'])
        msg['Subject'] = f"[{alert.severity.value.upper()}] MLB Data Service Alert: {alert.name}"
        
        body = f"""
MLB Data Service Alert

Alert: {alert.name}
Severity: {alert.severity.value.upper()}
Source: {alert.source}
Message: {alert.message}
Time: {alert.created_at.isoformat()}

Metric Value: {alert.metric_value}
Threshold: {alert.threshold}

Alert ID: {alert.id}

--
MLB Data Service Monitoring
        """
        
        msg.attach(MimeText(body, 'plain'))
        
        server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
        server.starttls()
        server.login(config['username'], config['password'])
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email notification sent for alert {alert.id}")
    
    def _send_webhook_notification(self, alert: Alert, channel: NotificationChannel):
        """Send webhook notification"""
        if not REQUESTS_AVAILABLE:
            logger.warning("Requests library not available, skipping webhook notification")
            return
            
        config = channel.config
        
        if config.get('format') == 'slack':
            payload = {
                "text": f"🚨 MLB Data Service Alert",
                "attachments": [{
                    "color": "danger" if alert.severity == AlertSeverity.CRITICAL else "warning",
                    "fields": [
                        {"title": "Alert", "value": alert.name, "short": True},
                        {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                        {"title": "Source", "value": alert.source, "short": True},
                        {"title": "Message", "value": alert.message, "short": False},
                        {"title": "Metric", "value": str(alert.metric_value), "short": True},
                        {"title": "Time", "value": alert.created_at.isoformat(), "short": True}
                    ]
                }]
            }
        else:
            payload = {
                "alert_id": alert.id,
                "name": alert.name,
                "severity": alert.severity.value,
                "message": alert.message,
                "source": alert.source,
                "metric_value": alert.metric_value,
                "threshold": alert.threshold,
                "created_at": alert.created_at.isoformat(),
                "metadata": alert.metadata
            }
        
        response = requests.post(
            config['url'],
            json=payload,
            timeout=10,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            logger.info(f"Webhook notification sent for alert {alert.id}")
        else:
            logger.error(f"Webhook notification failed: {response.status_code} - {response.text}")
    
    def _send_resolution_notification(self, alert: Alert):
        """Send notification when alert is resolved"""
        for channel in self.notification_channels:
            if not channel.enabled or alert.severity not in channel.severity_filter:
                continue
            
            try:
                if channel.type == 'email':
                    self._send_resolution_email(alert, channel)
                elif channel.type == 'webhook':
                    self._send_resolution_webhook(alert, channel)
            except Exception as e:
                logger.error(f"Failed to send resolution notification via {channel.name}: {e}")
    
    def _send_resolution_email(self, alert: Alert, channel: NotificationChannel):
        """Send email resolution notification"""
        config = channel.config
        
        msg = MimeMultipart()
        msg['From'] = config['from_address']
        msg['To'] = ', '.join(config['to_addresses'])
        msg['Subject'] = f"[RESOLVED] MLB Data Service Alert: {alert.name}"
        
        duration = alert.resolved_at - alert.created_at
        
        body = f"""
MLB Data Service Alert Resolved

Alert: {alert.name}
Original Severity: {alert.severity.value.upper()}
Source: {alert.source}
Resolution: {alert.metadata.get('resolution_message', 'Auto-resolved')}

Alert Duration: {duration}
Created: {alert.created_at.isoformat()}
Resolved: {alert.resolved_at.isoformat()}

Alert ID: {alert.id}

--
MLB Data Service Monitoring
        """
        
        msg.attach(MimeText(body, 'plain'))
        
        server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
        server.starttls()
        server.login(config['username'], config['password'])
        server.send_message(msg)
        server.quit()
    
    def _send_resolution_webhook(self, alert: Alert, channel: NotificationChannel):
        """Send webhook resolution notification"""
        if not REQUESTS_AVAILABLE:
            logger.warning("Requests library not available, skipping webhook resolution notification")
            return
            
        config = channel.config
        
        if config.get('format') == 'slack':
            payload = {
                "text": f"✅ MLB Data Service Alert Resolved",
                "attachments": [{
                    "color": "good",
                    "fields": [
                        {"title": "Alert", "value": alert.name, "short": True},
                        {"title": "Duration", "value": str(alert.resolved_at - alert.created_at), "short": True},
                        {"title": "Resolution", "value": alert.metadata.get('resolution_message', 'Auto-resolved'), "short": False}
                    ]
                }]
            }
        else:
            payload = {
                "event_type": "alert_resolved",
                "alert_id": alert.id,
                "name": alert.name,
                "severity": alert.severity.value,
                "resolution_message": alert.metadata.get('resolution_message', 'Auto-resolved'),
                "duration_seconds": (alert.resolved_at - alert.created_at).total_seconds(),
                "resolved_at": alert.resolved_at.isoformat()
            }
        
        requests.post(config['url'], json=payload, timeout=10)
    
    def _check_rate_limit(self, alert: Alert, channel: NotificationChannel) -> bool:
        """Check if notification rate limit allows sending"""
        if not alert.last_notification:
            return True
        
        rate_limit = self.rate_limits.get(alert.severity.value, timedelta(minutes=15))
        time_since_last = datetime.now() - alert.last_notification
        
        return time_since_last >= rate_limit
    
    def _persist_alert(self, alert: Alert):
        """Persist alert to SQLite database"""
        try:
            conn = sqlite3.connect(self.alert_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO alerts (
                    id, name, severity, state, message, source, metric_value,
                    threshold, created_at, updated_at, acknowledged_at, resolved_at,
                    acknowledged_by, metadata, notification_count, last_notification
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                alert.id, alert.name, alert.severity.value, alert.state.value,
                alert.message, alert.source, str(alert.metric_value),
                json.dumps(alert.threshold), alert.created_at.isoformat(),
                alert.updated_at.isoformat(),
                alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
                alert.resolved_at.isoformat() if alert.resolved_at else None,
                alert.acknowledged_by, json.dumps(alert.metadata),
                alert.notification_count,
                alert.last_notification.isoformat() if alert.last_notification else None
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to persist alert {alert.id}: {e}")
    
    def _load_active_alerts(self):
        """Load active alerts from storage"""
        try:
            conn = sqlite3.connect(self.alert_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM alerts WHERE state IN ('active', 'acknowledged')
                ORDER BY created_at DESC
            ''')
            
            for row in cursor.fetchall():
                alert = Alert(
                    id=row[0],
                    name=row[1],
                    severity=AlertSeverity(row[2]),
                    state=AlertState(row[3]),
                    message=row[4],
                    source=row[5],
                    metric_value=row[6],
                    threshold=json.loads(row[7]) if row[7] else {},
                    created_at=datetime.fromisoformat(row[8]),
                    updated_at=datetime.fromisoformat(row[9]),
                    acknowledged_at=datetime.fromisoformat(row[10]) if row[10] else None,
                    resolved_at=datetime.fromisoformat(row[11]) if row[11] else None,
                    acknowledged_by=row[12],
                    metadata=json.loads(row[13]) if row[13] else {},
                    notification_count=row[14] or 0,
                    last_notification=datetime.fromisoformat(row[15]) if row[15] else None
                )
                self.active_alerts[alert.id] = alert
            
            conn.close()
            logger.info(f"Loaded {len(self.active_alerts)} active alerts from storage")
            
        except Exception as e:
            logger.error(f"Failed to load active alerts: {e}")
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of current alert status"""
        active_count = len([a for a in self.active_alerts.values() if a.state == AlertState.ACTIVE])
        acknowledged_count = len([a for a in self.active_alerts.values() if a.state == AlertState.ACKNOWLEDGED])
        
        severity_counts = {
            'critical': len([a for a in self.active_alerts.values() if a.severity == AlertSeverity.CRITICAL]),
            'warning': len([a for a in self.active_alerts.values() if a.severity == AlertSeverity.WARNING]),
            'info': len([a for a in self.active_alerts.values() if a.severity == AlertSeverity.INFO])
        }
        
        return {
            'total_active': active_count,
            'acknowledged': acknowledged_count,
            'severity_breakdown': severity_counts,
            'notification_channels': len(self.notification_channels),
            'auto_recovery_enabled': self.auto_recovery_enabled,
            'alerts': [asdict(alert) for alert in self.active_alerts.values()]
        }
    
    def get_alert_history(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        """Get alert history for specified time period"""
        try:
            conn = sqlite3.connect(self.alert_db_path)
            cursor = conn.cursor()
            
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            cursor.execute('''
                SELECT * FROM alerts 
                WHERE created_at >= ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (cutoff_time.isoformat(), limit))
            
            alerts = []
            for row in cursor.fetchall():
                alert_dict = {
                    'id': row[0],
                    'name': row[1],
                    'severity': row[2],
                    'state': row[3],
                    'message': row[4],
                    'source': row[5],
                    'metric_value': row[6],
                    'threshold': json.loads(row[7]) if row[7] else {},
                    'created_at': row[8],
                    'updated_at': row[9],
                    'acknowledged_at': row[10],
                    'resolved_at': row[11],
                    'acknowledged_by': row[12],
                    'metadata': json.loads(row[13]) if row[13] else {},
                    'notification_count': row[14] or 0
                }
                alerts.append(alert_dict)
            
            conn.close()
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to get alert history: {e}")
            return []
    
    def _background_processor(self):
        """Background processing for alert lifecycle management"""
        while self._processing_active:
            try:
                # Check for stale alerts
                self._check_stale_alerts()
                
                # Clean up old resolved alerts
                self._cleanup_old_alerts()
                
                # Sleep for processing interval
                time.sleep(300)  # Process every 5 minutes
                
            except Exception as e:
                logger.error(f"Background processing error: {e}")
                time.sleep(60)
    
    def _check_stale_alerts(self):
        """Check for stale alerts that might need attention"""
        stale_threshold = timedelta(hours=2)
        now = datetime.now()
        
        for alert in self.active_alerts.values():
            if (alert.state == AlertState.ACTIVE and 
                now - alert.created_at > stale_threshold):
                logger.warning(f"Stale alert detected: {alert.id} ({alert.name})")
    
    def _cleanup_old_alerts(self):
        """Clean up resolved alerts older than retention period"""
        try:
            retention_days = 30
            cutoff_time = datetime.now() - timedelta(days=retention_days)
            
            conn = sqlite3.connect(self.alert_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM alerts 
                WHERE state = 'resolved' AND resolved_at < ?
            ''', (cutoff_time.isoformat(),))
            
            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old resolved alerts")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old alerts: {e}")
    
    def stop_processing(self):
        """Stop background processing"""
        self._processing_active = False
        if self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5)

if __name__ == "__main__":
    # Test the alert manager
    alert_manager = AlertManager()
    
    # Create test alert
    alert_id = alert_manager.create_alert(
        name="Test High CPU",
        severity=AlertSeverity.WARNING,
        message="CPU usage is high at 85%",
        source="health_monitor",
        metric_value=85.0,
        threshold={"warning": 70, "critical": 90}
    )
    
    print(f"Created test alert: {alert_id}")
    
    # Get alert summary
    summary = alert_manager.get_alert_summary()
    print(f"Alert summary: {summary}")
    
    # Clean up
    alert_manager.stop_processing()
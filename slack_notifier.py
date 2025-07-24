#!/usr/bin/env python3
"""
StatEdge Slack Notification Service
===================================

Sends real-time alerts and updates to Slack channels for StatEdge monitoring.
"""

import requests
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class StatEdgeSlackNotifier:
    """Enhanced Slack notifications for StatEdge monitoring system"""
    
    def __init__(self, webhook_url: str = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        
        if not self.webhook_url:
            logger.warning("No Slack webhook URL provided. Notifications disabled.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("StatEdge Slack notifications enabled")
    
    def send_notification(self, 
                         message: str, 
                         alert_type: str = "info",
                         channel: str = None,
                         metadata: Dict = None) -> bool:
        """Send notification to Slack with StatEdge branding"""
        
        if not self.enabled:
            logger.debug(f"Slack disabled. Would send: {message}")
            return False
        
        # Color coding for different alert types
        colors = {
            "success": "#28a745",  # Green
            "warning": "#ffc107",  # Yellow  
            "error": "#dc3545",    # Red
            "info": "#007bff",     # Blue
            "critical": "#721c24"  # Dark red
        }
        
        # Emoji mapping
        emojis = {
            "success": ":white_check_mark:",
            "warning": ":warning:",
            "error": ":x:",
            "info": ":information_source:",
            "critical": ":rotating_light:"
        }
        
        # Build Slack payload
        payload = {
            "username": "StatEdge Bot",
            "icon_emoji": ":chart_with_upwards_trend:",
            "attachments": [{
                "color": colors.get(alert_type, "#000000"),
                "title": f"{emojis.get(alert_type, ':bell:')} StatEdge Alert",
                "text": message,
                "footer": "StatEdge MLB Analytics Platform",
                "footer_icon": "https://statedge.app/favicon.ico",
                "ts": int(datetime.now().timestamp()),
                "fields": []
            }]
        }
        
        # Add metadata fields if provided
        if metadata:
            for key, value in metadata.items():
                payload["attachments"][0]["fields"].append({
                    "title": key.replace('_', ' ').title(),
                    "value": str(value),
                    "short": True
                })
        
        # Override channel if specified
        if channel:
            payload["channel"] = channel
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug(f"Slack notification sent successfully: {alert_type}")
                return True
            else:
                logger.error(f"Slack notification failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def system_startup(self, metrics: Dict = None):
        """Notify when StatEdge system starts up"""
        message = f"🚀 *StatEdge is online!*\n\nMLB Analytics Platform is ready for action."
        
        startup_data = {
            "Status": "Operational",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "Dashboard": "http://localhost:8081/monitoring"
        }
        
        if metrics:
            startup_data.update(metrics)
        
        return self.send_notification(message, "success", metadata=startup_data)
    
    def database_alert(self, issue: str, severity: str = "warning", stats: Dict = None):
        """Alert about database issues"""
        message = f"💾 *Database Alert*\n\n{issue}"
        
        db_data = {"Severity": severity.title()}
        if stats:
            db_data.update(stats)
        
        return self.send_notification(message, severity, metadata=db_data)
    
    def data_collection_complete(self, source: str, records: int, duration: str = None):
        """Notify when data collection completes"""
        message = f"📊 *Data Collection Complete*\n\n{source}: {records:,} records collected"
        
        collection_data = {
            "Source": source,
            "Records": f"{records:,}",
            "Status": "Success"
        }
        
        if duration:
            collection_data["Duration"] = duration
        
        return self.send_notification(message, "success", metadata=collection_data)
    
    def data_collection_failed(self, source: str, error: str):
        """Alert when data collection fails"""
        message = f"❌ *Data Collection Failed*\n\n{source}: {error}"
        
        return self.send_notification(message, "error", metadata={
            "Source": source,
            "Error": error[:100] + "..." if len(error) > 100 else error
        })
    
    def performance_alert(self, metric: str, value: float, threshold: float):
        """Alert about performance issues"""
        message = f"⚡ *Performance Alert*\n\n{metric}: {value}% (threshold: {threshold}%)"
        
        return self.send_notification(message, "warning", metadata={
            "Metric": metric,
            "Current Value": f"{value}%",
            "Threshold": f"{threshold}%"
        })
    
    def critical_system_alert(self, issue: str, details: Dict = None):
        """Send critical system alerts"""
        message = f"🚨 *CRITICAL SYSTEM ALERT*\n\n{issue}"
        
        alert_data = {"Priority": "CRITICAL", "Action Required": "Immediate"}
        if details:
            alert_data.update(details)
        
        return self.send_notification(message, "critical", metadata=alert_data)
    
    def daily_report(self, stats: Dict):
        """Send daily StatEdge report"""
        message = "📈 *Daily StatEdge Report*\n\nToday's system performance summary"
        
        return self.send_notification(message, "info", metadata=stats)
    
    def api_usage_alert(self, endpoint: str, usage_count: int, limit: int):
        """Alert about API usage approaching limits"""
        percentage = (usage_count / limit) * 100
        
        message = f"📡 *API Usage Alert*\n\n{endpoint}: {usage_count}/{limit} calls ({percentage:.1f}%)"
        
        return self.send_notification(message, "warning", metadata={
            "Endpoint": endpoint,
            "Usage": f"{usage_count:,}/{limit:,}",
            "Percentage": f"{percentage:.1f}%"
        })
    
    def test_notification(self):
        """Send a test notification"""
        message = "🧪 *StatEdge Test Notification*\n\nSlack integration is working perfectly!"
        
        return self.send_notification(message, "info", metadata={
            "Test Type": "Integration Test",
            "System": "StatEdge",
            "Status": "Success"
        })


# Convenience function for quick access
def get_slack_notifier() -> StatEdgeSlackNotifier:
    """Get configured Slack notifier instance"""
    return StatEdgeSlackNotifier()


if __name__ == "__main__":
    # Test the Slack notifier
    notifier = StatEdgeSlackNotifier()
    
    if notifier.enabled:
        print("Testing Slack notifications...")
        
        # Test different types of notifications
        notifier.test_notification()
        notifier.system_startup({"Database Records": "474,483", "Uptime": "99.9%"})
        notifier.data_collection_complete("FanGraphs Batting", 1323, "45 seconds")
        
        print("Test notifications sent!")
    else:
        print("Slack webhook URL not configured. Set SLACK_WEBHOOK_URL environment variable.")
#!/usr/bin/env python3
"""
Test Slack Integration for StatEdge
"""

import os
import sys
from slack_notifier import StatEdgeSlackNotifier

def test_slack_integration():
    """Test Slack webhook integration"""
    
    webhook_url = input("🔗 Enter your Slack webhook URL: ").strip()
    
    if not webhook_url.startswith("https://hooks.slack.com/"):
        print("❌ Invalid webhook URL. Should start with https://hooks.slack.com/")
        return False
    
    # Set environment variable for testing
    os.environ['SLACK_WEBHOOK_URL'] = webhook_url
    
    # Create notifier
    notifier = StatEdgeSlackNotifier(webhook_url)
    
    if not notifier.enabled:
        print("❌ Slack notifier not enabled")
        return False
    
    print("\n🧪 Testing Slack notifications...")
    
    # Test 1: Basic test notification
    print("1. Sending test notification...")
    success1 = notifier.test_notification()
    print(f"   {'✅ Success' if success1 else '❌ Failed'}")
    
    # Test 2: System startup notification
    print("2. Sending startup notification...")
    success2 = notifier.system_startup({
        "Database Records": "474,483",
        "Docker": "Running",
        "Status": "Testing"
    })
    print(f"   {'✅ Success' if success2 else '❌ Failed'}")
    
    # Test 3: Performance alert
    print("3. Sending performance alert...")
    success3 = notifier.performance_alert("CPU Usage", 85.5, 80)
    print(f"   {'✅ Success' if success3 else '❌ Failed'}")
    
    # Test 4: Data collection complete
    print("4. Sending data collection notification...")
    success4 = notifier.data_collection_complete("FanGraphs Batting", 1323, "45 seconds")
    print(f"   {'✅ Success' if success4 else '❌ Failed'}")
    
    all_success = all([success1, success2, success3, success4])
    
    print(f"\n🎯 Overall result: {'✅ All tests passed!' if all_success else '❌ Some tests failed'}")
    
    if all_success:
        print("\n✅ Slack integration is working perfectly!")
        print(f"🔧 To enable in Docker, run:")
        print(f"   export SLACK_WEBHOOK_URL='{webhook_url}'")
        print(f"   docker-compose -f docker-compose.statedge.yml up -d")
        
        # Save to .env file for convenience
        with open('.env.slack', 'w') as f:
            f.write(f"SLACK_WEBHOOK_URL={webhook_url}\n")
        print(f"💾 Webhook URL saved to .env.slack file")
    
    return all_success

if __name__ == "__main__":
    print("🏢 StatEdge Slack Integration Test")
    print("=" * 40)
    print("This will send test notifications to your Slack channel.")
    print()
    
    result = test_slack_integration()
    sys.exit(0 if result else 1)
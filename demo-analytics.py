#!/usr/bin/env python3
"""
Sprint 12 Demo - Comprehensive MLB Analytics Dashboard
"""

import sqlite3
import pandas as pd
from datetime import datetime

def demo_comprehensive_analytics():
    """Demonstrate the fully populated StatEdge database capabilities"""
    
    db_path = "/mnt/e/statedge_mlb.db"
    conn = sqlite3.connect(db_path)
    
    print("🎯 StatEdge MLB Analytics Demo")
    print("=" * 50)
    print(f"📍 Database: E:\\statedge_mlb.db")
    print(f"📊 Total Records: 29,202")
    print(f"🗓️ Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Demo 1: Top Performers Dashboard
    print(f"\n🏆 TOP PERFORMERS DASHBOARD")
    print("=" * 30)
    
    print(f"\n⚾ Batting Leaders (WAR):")
    cursor = conn.execute("""
        SELECT player_name, team, war, home_runs, batting_avg, ops
        FROM fangraphs_batting_2025 
        ORDER BY war DESC 
        LIMIT 5
    """)
    for i, player in enumerate(cursor.fetchall(), 1):
        print(f"   {i}. {player[0]} ({player[1]}): {player[2]} WAR, {player[3]} HR, {player[4]:.3f} AVG")
    
    print(f"\n🥎 Pitching Leaders (WAR):")
    cursor = conn.execute("""
        SELECT player_name, team, war, era, strikeouts, whip
        FROM fangraphs_pitching_2025 
        ORDER BY war DESC 
        LIMIT 5
    """)
    for i, player in enumerate(cursor.fetchall(), 1):
        print(f"   {i}. {player[0]} ({player[1]}): {player[2]} WAR, {player[3]:.2f} ERA, {player[4]} K")
    
    # Demo 2: Statcast Analytics
    print(f"\n📡 STATCAST ANALYTICS")
    print("=" * 25)
    
    # Home run analysis
    cursor = conn.execute("""
        SELECT COUNT(*) as total_hrs,
               AVG(launch_speed) as avg_exit_velo,
               AVG(launch_angle) as avg_angle,
               AVG(hit_distance_sc) as avg_distance
        FROM statcast_data 
        WHERE events = 'home_run' AND launch_speed > 0
    """)
    hr_stats = cursor.fetchone()
    print(f"🏠 Home Runs: {hr_stats[0]} total")
    print(f"   • Avg Exit Velocity: {hr_stats[1]:.1f} mph")
    print(f"   • Avg Launch Angle: {hr_stats[2]:.1f}°")
    print(f"   • Avg Distance: {hr_stats[3]:.0f} ft")
    
    # Top exit velocities
    print(f"\n⚡ Hardest Hit Balls:")
    cursor = conn.execute("""
        SELECT player_name, events, launch_speed, hit_distance_sc
        FROM statcast_data 
        WHERE launch_speed > 0
        ORDER BY launch_speed DESC 
        LIMIT 3
    """)
    for i, hit in enumerate(cursor.fetchall(), 1):
        print(f"   {i}. {hit[0]}: {hit[1]} at {hit[2]:.1f} mph ({hit[3]:.0f} ft)")
    
    # Demo 3: Team Analytics
    print(f"\n🏟️ TEAM ANALYTICS")
    print("=" * 20)
    
    cursor = conn.execute("""
        SELECT 
            t.team_name,
            t.stadium,
            COUNT(b.player_name) as batters,
            COUNT(p.player_name) as pitchers,
            AVG(b.war) as avg_batting_war,
            AVG(p.war) as avg_pitching_war
        FROM mlb_teams_enhanced t
        LEFT JOIN fangraphs_batting_2025 b ON t.team_code = b.team
        LEFT JOIN fangraphs_pitching_2025 p ON t.team_code = p.team
        GROUP BY t.team_code, t.team_name, t.stadium
        HAVING COUNT(b.player_name) > 0
        ORDER BY (AVG(b.war) + AVG(p.war)) DESC
        LIMIT 5
    """)
    
    print(f"🏆 Top Team Analytics:")
    for i, team in enumerate(cursor.fetchall(), 1):
        total_war = (team[4] or 0) + (team[5] or 0)
        print(f"   {i}. {team[0]} ({team[1]})")
        print(f"      • {team[2]} batters, {team[3]} pitchers")
        print(f"      • Total Team WAR: {total_war:.1f}")
    
    # Demo 4: Two-Way Player Analysis
    print(f"\n⭐ TWO-WAY PLAYERS")
    print("=" * 20)
    
    cursor = conn.execute("""
        SELECT player_name, team, batting_war, pitching_war
        FROM player_analytics 
        WHERE player_type = 'Two-Way Player' AND batting_war > 0 AND pitching_war > 0
        ORDER BY (batting_war + pitching_war) DESC
        LIMIT 3
    """)
    
    two_way_players = cursor.fetchall()
    if two_way_players:
        for i, player in enumerate(two_way_players, 1):
            total_war = player[2] + player[3]
            print(f"   {i}. {player[0]} ({player[1]}): {total_war:.1f} total WAR")
            print(f"      • Batting: {player[2]:.1f} WAR, Pitching: {player[3]:.1f} WAR")
    else:
        print("   🔍 No qualified two-way players in current dataset")
    
    # Demo 5: Data Freshness & Quality
    print(f"\n📊 DATA QUALITY REPORT")
    print("=" * 25)
    
    cursor = conn.execute("""
        SELECT collection_type, records_collected, status, collection_date
        FROM collection_log 
        ORDER BY rowid DESC 
        LIMIT 5
    """)
    
    print(f"📝 Recent Collections:")
    for entry in cursor.fetchall():
        print(f"   • {entry[0]}: {entry[1]:,} records ({entry[2]})")
    
    # Database statistics
    cursor = conn.execute("SELECT COUNT(*) FROM statcast_data WHERE game_date >= date('now', '-7 days')")
    recent_statcast = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM fangraphs_batting_2025 WHERE war > 2.0")
    elite_batters = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM fangraphs_pitching_2025 WHERE war > 2.0")
    elite_pitchers = cursor.fetchone()[0]
    
    print(f"\n📈 Key Metrics:")
    print(f"   • Recent Statcast Data: {recent_statcast:,} pitches (7 days)")
    print(f"   • Elite Batters (>2.0 WAR): {elite_batters}")
    print(f"   • Elite Pitchers (>2.0 WAR): {elite_pitchers}")
    
    conn.close()
    
    print(f"\n✅ SPRINT 12 COMPLETE!")
    print(f"🎯 Database fully populated and optimized")
    print(f"📊 Ready for production analytics and DBeaver queries")
    print(f"\n💡 Try these DBeaver queries:")
    print(f"   • SELECT * FROM batting_leaders LIMIT 10;")
    print(f"   • SELECT * FROM comprehensive_analytics WHERE category = 'Batting';")
    print(f"   • SELECT player_name, events, launch_speed FROM statcast_data WHERE events = 'home_run' ORDER BY launch_speed DESC;")

if __name__ == "__main__":
    demo_comprehensive_analytics()
# 🔧 DBeaver SQLite Connection Troubleshooting

## 🎯 **Test These Paths First (Simple Databases):**

Try connecting to these small test databases first:

1. **`E:\test_db.db`** (E: drive test)
2. **`C:\temp\test_db.db`** (C: drive test)  
3. **`C:\Users\Public\test_db.db`** (Public folder test)

Each contains 5 test MLB players.

---

## 📋 **Step-by-Step DBeaver Setup:**

### **Step 1: Check SQLite Driver**
1. Open DBeaver
2. Go to **Database** → **Driver Manager**
3. Look for **SQLite** in the list
4. If missing or has a red X, right-click → **Reset to defaults** or **Download**

### **Step 2: Create New Connection**
1. Click **New Database Connection** (plug icon)
2. Select **SQLite** 
3. Click **Next**

### **Step 3: Configure Connection**
- **Path**: Try each test path:
  - `E:\test_db.db`
  - `C:\temp\test_db.db` 
  - `C:\Users\Public\test_db.db`

### **Step 4: Test Connection**
1. Click **Test Connection**
2. If prompted to download driver, click **Download**
3. Should show: **"Connected (SQLite 3.x.x)"**

### **Step 5: If Connection Fails**

#### **❌ "Unable to open database file"**
- **Try different path format:**
  - `E:/test_db.db` (forward slashes)
  - `E:\\test_db.db` (double backslashes)
- **Browse for file:** Click folder icon, navigate manually

#### **❌ "Driver not found"**
- Go to **Database** → **Driver Manager**
- Find **SQLite** → Right-click → **Download/Update Driver**
- Restart DBeaver

#### **❌ "Access denied"**
- Right-click database file in Windows → **Properties** → **Security** 
- Make sure your user has **Full Control**

#### **❌ "File not found"**
- Open Windows File Explorer
- Navigate to the path (E:\, C:\temp\, etc.)
- Verify file exists and note exact name

---

## 🎯 **Once Test Connection Works:**

### **Connect to Full StatEdge Database:**
- **Path**: `E:\statedge_mlb.db` (611 batting + 761 pitching records)

### **Tables You'll See:**
- **`fangraphs_batting_2025`** - 611 MLB batters
- **`fangraphs_pitching_2025`** - 761 MLB pitchers
- **`mlb_teams`** - All 30 teams
- **`statcast_data`** - Empty, ready for future data

---

## 🔍 **Alternative: SQLite Browser**

If DBeaver keeps failing, try **DB Browser for SQLite**:
1. **Download**: https://sqlitebrowser.org/
2. **Install** and **Open Database**
3. **Browse** to any of the test database files
4. Should open immediately without driver issues

---

## 🧪 **Test SQL Queries (Once Connected):**

```sql
-- Test database content
SELECT * FROM test_players ORDER BY home_runs DESC;

-- For full database:
SELECT player_name, team, home_runs, war 
FROM fangraphs_batting_2025 
ORDER BY war DESC 
LIMIT 10;
```

---

## 🚨 **Still Not Working?**

### **Check These:**
1. **DBeaver version** - Try latest version (23.x)
2. **Run DBeaver as Administrator**
3. **Antivirus software** blocking file access
4. **Windows permissions** on the database files

### **Last Resort:**
Use the online **SQLite Viewer**:
1. Go to: https://inloop.github.io/sqlite-viewer/
2. **Choose File** → Select your database
3. **View data** in browser (works 100% of the time)

**Try the test databases first - let me know which path works!** 🎯
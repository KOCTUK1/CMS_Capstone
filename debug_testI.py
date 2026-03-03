from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

# Don't reload - use current page with schedule visible
print(f"URL: {d.current_url}")

# Explore the full vems object for any data stores
print("\n=== Exploring vems namespace ===")
result = d.execute_script("""
    if (typeof vems === 'undefined') return {error: 'vems not found'};
    var keys = Object.keys(vems);
    return keys;
""")
print(f"vems keys: {result}")

# Check each sub-object for arrays that might contain booking data
print("\n=== Checking vems sub-objects for data ===")
for key in result:
    try:
        info = d.execute_script(f"""
            var obj = vems['{key}'];
            if (!obj) return null;
            var type = typeof obj;
            if (type === 'function') return {{type: 'function'}};
            if (type === 'object') {{
                var subkeys = Object.keys(obj).slice(0, 15);
                return {{type: 'object', keys: subkeys}};
            }}
            return {{type: type, value: String(obj).substring(0, 100)}};
        """)
        if info:
            print(f"  vems.{key}: {json.dumps(info)}")
    except:
        pass

# Try to find the room schedule data specifically
print("\n=== Looking for schedule/room data in Knockout observables ===")
schedule_data = d.execute_script("""
    // Look for KO view models bound to the grid area
    var gridEl = document.querySelector('.booking-grid, [class*="schedule"], [class*="grid"]');
    if (gridEl) {
        var ctx = ko.contextFor(gridEl);
        if (ctx && ctx.$data) {
            var data = ctx.$data;
            var keys = Object.keys(data).slice(0, 30);
            return {element: gridEl.className, keys: keys};
        }
    }
    return {error: 'no KO context found on grid'};
""")
print(json.dumps(schedule_data, indent=2))

# Try to get data from div.event elements directly
print("\n=== Reading data from div.event elements via Knockout context ===")
event_data = d.execute_script("""
    var events = document.querySelectorAll('div.event');
    var results = [];
    for (var i = 0; i < Math.min(events.length, 10); i++) {
        try {
            var ctx = ko.contextFor(events[i]);
            if (ctx && ctx.$data) {
                var d = ctx.$data;
                var info = {};
                var keys = Object.keys(d);
                for (var k = 0; k < keys.length; k++) {
                    var val = d[keys[k]];
                    if (typeof val === 'function') {
                        try { info[keys[k]] = val(); } catch(e) {}
                    } else {
                        info[keys[k]] = val;
                    }
                }
                results.push(JSON.stringify(info).substring(0, 500));
            }
        } catch(e) {
            results.push('error: ' + e.message);
        }
    }
    return results;
""")
print(f"Found data for {len(event_data)} events:")
for i, ev in enumerate(event_data):
    print(f"\n  [{i}] {ev}")

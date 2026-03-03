from selenium import webdriver
from selenium.webdriver.edge.options import Options
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

print(f"URL: {d.current_url}")

# Get roomResults data
print("\n=== roomResults ===")
result = d.execute_script("""
    var gridEl = document.querySelector('.booking-grid');
    if (!gridEl) return {error: 'no grid'};
    var ctx = ko.contextFor(gridEl);
    if (!ctx || !ctx.$data) return {error: 'no context'};
    
    var rr = ctx.$data.roomResults;
    if (!rr) return {error: 'no roomResults'};
    
    var data = typeof rr === 'function' ? rr() : rr;
    if (!data) return {error: 'roomResults is empty'};
    
    return {
        type: typeof data,
        isArray: Array.isArray(data),
        length: Array.isArray(data) ? data.length : null,
        sample: JSON.stringify(data).substring(0, 2000)
    };
""")
print(json.dumps(result, indent=2))

# Get listRoomResults
print("\n=== listRoomResults ===")
result2 = d.execute_script("""
    var gridEl = document.querySelector('.booking-grid');
    var ctx = ko.contextFor(gridEl);
    var lr = ctx.$data.listRoomResults;
    if (!lr) return {error: 'no listRoomResults'};
    
    var data = typeof lr === 'function' ? lr() : lr;
    if (!data) return {error: 'empty'};
    
    return {
        type: typeof data,
        isArray: Array.isArray(data),
        length: Array.isArray(data) ? data.length : null,
        sample: JSON.stringify(data).substring(0, 2000)
    };
""")
print(json.dumps(result2, indent=2))

# Try to get the first room's bookings/events
print("\n=== First few rooms with their events ===")
rooms = d.execute_script("""
    var gridEl = document.querySelector('.booking-grid');
    var ctx = ko.contextFor(gridEl);
    var rr = ctx.$data.roomResults;
    var data = typeof rr === 'function' ? rr() : rr;
    if (!data || !Array.isArray(data)) return [];
    
    var results = [];
    for (var i = 0; i < Math.min(data.length, 5); i++) {
        var room = data[i];
        var keys = Object.keys(room);
        var info = {};
        for (var k = 0; k < keys.length; k++) {
            var val = room[keys[k]];
            if (typeof val === 'function') {
                try { val = val(); } catch(e) { val = 'fn_error'; }
            }
            if (Array.isArray(val)) {
                info[keys[k]] = {isArray: true, length: val.length};
                if (val.length > 0) {
                    // Get first item details
                    var first = val[0];
                    var firstKeys = Object.keys(first);
                    var firstInfo = {};
                    for (var j = 0; j < firstKeys.length; j++) {
                        var fv = first[firstKeys[j]];
                        if (typeof fv === 'function') {
                            try { fv = fv(); } catch(e) { fv = 'fn_error'; }
                        }
                        firstInfo[firstKeys[j]] = fv;
                    }
                    info[keys[k]].firstItem = JSON.stringify(firstInfo).substring(0, 500);
                }
            } else {
                info[keys[k]] = val;
            }
        }
        results.push(JSON.stringify(info).substring(0, 800));
    }
    return results;
""")
for i, r in enumerate(rooms):
    print(f"\n  Room [{i}]: {r}")

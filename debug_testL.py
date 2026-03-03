from selenium import webdriver
from selenium.webdriver.edge.options import Options
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

# Don't reload - just read what's on screen
count = d.execute_script("return document.querySelectorAll('div.event').length;")
print(f"div.event count: {count}")

if count == 0:
    print("No events visible. Manually click Search in Edge, wait for blue blocks, then run this again.")
    exit()

# Get API URL
api_url = d.execute_script("return vems.serverApiUrl();")
print(f"API URL: {api_url}")

# Get roomResults
rr_len = d.execute_script("""
    var gridEl = document.querySelector('.booking-grid');
    var ctx = ko.contextFor(gridEl);
    var rr = ctx.$data.roomResults;
    var data = typeof rr === 'function' ? rr() : rr;
    return Array.isArray(data) ? data.length : -1;
""")
print(f"roomResults length: {rr_len}")

# Get cookies for API calls
cookies = d.execute_script("return document.cookie;")
print(f"\nCookies (first 200): {cookies[:200]}")

# Try calling the API directly from the browser
print("\n=== Trying direct API call ===")
api_result = d.execute_script("""
    return new Promise(function(resolve) {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', arguments[0] + 'GetBrowseLocationsRoomsForRoomRequest', true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.onload = function() {
            resolve({status: xhr.status, response: xhr.responseText.substring(0, 2000)});
        };
        xhr.onerror = function() {
            resolve({error: 'request failed'});
        };
        xhr.send(JSON.stringify({
            filterData: {
                DateRange: ["2026-03-11"],
                TimeRange: {Start: "12:00 AM", End: "11:59 PM"}
            }
        }));
    });
""")
print(json.dumps(api_result, indent=2) if api_result else "No result (async didn't complete)")

# Wait for async
time.sleep(5)
api_result = d.execute_script("return window._lastApiResult || null;")

# Alternative: just read event data from each div.event's parent chain
print("\n=== Reading event-container data ===")
containers = d.execute_script("""
    var containers = document.querySelectorAll('.event-container');
    var results = [];
    for (var i = 0; i < Math.min(containers.length, 5); i++) {
        var ev = containers[i].querySelector('.event');
        var style = ev ? ev.getAttribute('style') : '';
        var parent = containers[i].parentElement;
        var grandparent = parent ? parent.parentElement : null;
        
        // Walk up to find room info
        var rowEl = containers[i];
        for (var d = 0; d < 10; d++) {
            if (!rowEl) break;
            if (rowEl.className && rowEl.className.includes('room')) break;
            rowEl = rowEl.parentElement;
        }
        
        results.push({
            containerClass: containers[i].className,
            parentClass: parent ? parent.className.substring(0, 80) : '',
            grandparentClass: grandparent ? grandparent.className.substring(0, 80) : '',
            rowClass: rowEl ? rowEl.className.substring(0, 80) : '',
            rowText: rowEl ? rowEl.textContent.substring(0, 100) : '',
            style: style
        });
    }
    return results;
""")
for i, c in enumerate(containers):
    print(f"\n  [{i}]")
    for k, v in c.items():
        print(f"    {k}: {v}")

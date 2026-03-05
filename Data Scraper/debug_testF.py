from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

# Inject AJAX interceptor BEFORE clicking search
print("Injecting AJAX interceptor...")
d.execute_script("""
    window._capturedRequests = [];
    var origOpen = XMLHttpRequest.prototype.open;
    var origSend = XMLHttpRequest.prototype.send;
    
    XMLHttpRequest.prototype.open = function(method, url) {
        this._url = url;
        this._method = method;
        return origOpen.apply(this, arguments);
    };
    
    XMLHttpRequest.prototype.send = function(body) {
        var xhr = this;
        xhr._body = body;
        var origHandler = xhr.onreadystatechange;
        xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {
                window._capturedRequests.push({
                    url: xhr._url,
                    method: xhr._method,
                    status: xhr.status,
                    body: xhr._body ? xhr._body.substring(0, 500) : '',
                    response: xhr.responseText ? xhr.responseText.substring(0, 1000) : ''
                });
            }
            if (origHandler) origHandler.apply(this, arguments);
        };
        return origSend.apply(this, arguments);
    };
    console.log('AJAX interceptor installed');
""")

# Click Search
print("Clicking Search...")
for s in d.find_elements(By.CSS_SELECTOR, "button.find-a-room"):
    if s.is_displayed():
        s.click()
        break

# Also click Schedule tab
time.sleep(2)
try:
    tabs = d.find_elements(By.XPATH, "//a[text()='Schedule'] | //li[contains(text(),'Schedule')] | //*[contains(@class,'schedule-tab')]")
    for t in tabs:
        if t.is_displayed():
            print(f"Clicking Schedule tab: {t.tag_name} text={t.text}")
            t.click()
            break
except:
    pass

# Scroll down to the grid area
print("Scrolling to grid...")
d.execute_script("window.scrollTo(0, 900);")
time.sleep(3)

# Wait and collect AJAX requests
print("Waiting 15 seconds for AJAX requests...")
time.sleep(15)

# Check captured requests
requests = d.execute_script("return window._capturedRequests || [];")
print(f"\n=== Captured {len(requests)} AJAX requests ===")
for i, req in enumerate(requests):
    print(f"\n[{i}] {req['method']} {req['url']}")
    print(f"    status: {req['status']}")
    if req['body']:
        print(f"    body: {req['body'][:200]}")
    if req['response']:
        print(f"    response: {req['response'][:300]}")

# Also check what the Knockout view model has
print("\n=== Knockout ViewModel data ===")
try:
    vm_data = d.execute_script("""
        if (typeof vems !== 'undefined' && vems.roomRequest) {
            var rr = vems.roomRequest;
            return {
                pageMode: rr.pageMode,
                hasRooms: typeof rr.rooms !== 'undefined',
                hasResults: typeof rr.searchResults !== 'undefined',
                keys: Object.keys(rr).slice(0, 30)
            };
        }
        return {error: 'vems.roomRequest not found'};
    """)
    print(json.dumps(vm_data, indent=2))
except Exception as e:
    print(f"Error: {e}")

# Try to access the room/booking data from the Knockout model
print("\n=== Trying to read booking data from ViewModel ===")
try:
    booking_data = d.execute_script("""
        if (typeof vems !== 'undefined' && vems.roomRequest) {
            var rr = vems.roomRequest;
            // Try common property names
            var props = ['rooms', 'searchResults', 'browseResults', 'locations', 
                         'availableRooms', 'roomResults', 'scheduleData', 'bookings',
                         'events', 'reservations', 'gridData'];
            var found = {};
            for (var i = 0; i < props.length; i++) {
                var val = rr[props[i]];
                if (val) {
                    if (typeof val === 'function') {
                        var result = val();
                        found[props[i]] = {
                            type: typeof result,
                            isArray: Array.isArray(result),
                            length: Array.isArray(result) ? result.length : null,
                            sample: JSON.stringify(result).substring(0, 300)
                        };
                    } else {
                        found[props[i]] = {
                            type: typeof val,
                            value: JSON.stringify(val).substring(0, 300)
                        };
                    }
                }
            }
            return found;
        }
        return {error: 'not found'};
    """)
    print(json.dumps(booking_data, indent=2))
except Exception as e:
    print(f"Error: {e}")

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

# Load page fresh
d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

# Click Search with a real mouse action
print("Clicking Search with ActionChains...")
btn = d.find_element(By.CSS_SELECTOR, "button.find-a-room")
ActionChains(d).move_to_element(btn).pause(1).click().perform()

# Wait for div.event to appear
print("Waiting for events to load...")
for i in range(40):
    time.sleep(2)
    count = d.execute_script("return document.querySelectorAll('div.event').length;")
    if count > 0:
        print(f"  Found {count} events after {(i+1)*2}s")
        time.sleep(3)  # Extra time
        break
    if i % 5 == 4:
        print(f"  Still waiting... {(i+1)*2}s")

# Now check roomResults again
print("\n=== roomResults after search ===")
result = d.execute_script("""
    var gridEl = document.querySelector('.booking-grid');
    if (!gridEl) return {error: 'no grid'};
    var ctx = ko.contextFor(gridEl);
    if (!ctx || !ctx.$data) return {error: 'no context'};
    var rr = ctx.$data.roomResults;
    var data = typeof rr === 'function' ? rr() : rr;
    return {
        length: Array.isArray(data) ? data.length : 'not array',
        sample: JSON.stringify(data).substring(0, 500)
    };
""")
print(json.dumps(result, indent=2))

# Read the Knockout context from event-container (parent of div.event)
print("\n=== Knockout context from event-container ===")
container_data = d.execute_script("""
    var containers = document.querySelectorAll('.event-container');
    if (containers.length === 0) return {error: 'no event-containers'};
    
    var results = [];
    for (var i = 0; i < Math.min(containers.length, 3); i++) {
        try {
            var ctx = ko.contextFor(containers[i]);
            if (ctx && ctx.$data) {
                var keys = Object.keys(ctx.$data);
                results.push({
                    index: i,
                    keys: keys.slice(0, 20),
                    sample: JSON.stringify(ctx.$data).substring(0, 500)
                });
            }
        } catch(e) {
            results.push({index: i, error: e.message});
        }
    }
    return results;
""")
print(json.dumps(container_data, indent=2))

# Try reading data-bind attributes from event and its parents
print("\n=== data-bind on div.event and parents ===")
bindings = d.execute_script("""
    var ev = document.querySelector('div.event');
    if (!ev) return {error: 'no div.event'};
    
    var chain = [];
    var el = ev;
    for (var depth = 0; depth < 8; depth++) {
        if (!el) break;
        var db = el.getAttribute('data-bind') || '';
        chain.push({
            tag: el.tagName,
            class: el.className.substring(0, 80),
            dataBind: db.substring(0, 200),
        });
        el = el.parentElement;
    }
    return chain;
""")
for item in bindings:
    print(f"  {item['tag']} class={item['class']}")
    if item['dataBind']:
        print(f"    data-bind: {item['dataBind']}")

# Try getting ALL bookings data from the browse VM
print("\n=== vems.browse deep inspection ===")
browse = d.execute_script("""
    if (!vems.browse) return {error: 'no browse'};
    var keys = Object.keys(vems.browse);
    var info = {};
    for (var i = 0; i < keys.length; i++) {
        var val = vems.browse[keys[i]];
        if (typeof val === 'function') {
            try {
                var result = val();
                if (Array.isArray(result) && result.length > 0) {
                    info[keys[i]] = {type: 'array', length: result.length, 
                        sample: JSON.stringify(result[0]).substring(0, 300)};
                } else {
                    info[keys[i]] = {type: typeof result, value: String(result).substring(0, 50)};
                }
            } catch(e) {}
        }
    }
    return info;
""")
print(json.dumps(browse, indent=2))

# Nuclear option: just get the API URL so we can call it directly
print("\n=== Server API URL ===")
api_url = d.execute_script("return vems.serverApiUrl();")
print(f"API URL: {api_url}")

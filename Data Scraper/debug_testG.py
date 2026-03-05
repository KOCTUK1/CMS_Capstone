from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time, json

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

# DON'T reload the page - just use whatever is currently showing
# (you should have the schedule visible with blue blocks)

print("=== Current page check ===")
print(f"URL: {d.current_url}")
body = d.find_element(By.TAG_NAME, "body").text
print(f"Has 'Rooms You Can Request': {'Rooms You Can Request' in body}")

# Find div.event elements
print("\n=== Looking for div.event ===")
events = d.find_elements(By.CSS_SELECTOR, "div.event")
print(f"Found {len(events)} div.event elements")

if not events:
    # Maybe we need to scroll or the page needs to load
    print("Trying to find events via JavaScript...")
    count = d.execute_script("return document.querySelectorAll('div.event').length;")
    print(f"JS found {count} div.event elements")
    
    # Also check .event (without div)
    count2 = d.execute_script("return document.querySelectorAll('.event').length;")
    print(f"JS found {count2} .event elements")
    
    # Check how many are visible
    visible = d.execute_script("""
        var evts = document.querySelectorAll('div.event');
        var vis = 0;
        for (var i = 0; i < evts.length; i++) {
            if (evts[i].offsetParent !== null) vis++;
        }
        return {total: evts.length, visible: vis};
    """)
    print(f"Total: {visible['total']}, Visible: {visible['visible']}")

# If we found events, inspect the first few
if events:
    for i, ev in enumerate(events[:5]):
        w = ev.size.get("width", 0)
        h = ev.size.get("height", 0)
        vis = ev.is_displayed()
        style = ev.get_attribute("style") or ""
        parent_cls = d.execute_script("return arguments[0].parentElement.className;", ev)
        print(f"  [{i}] visible={vis} size={w}x{h} style={style} parent_class={parent_cls}")

# Even if Selenium can't find them, try clicking via JS
print("\n=== Trying to click first visible div.event via JS ===")
click_result = d.execute_script("""
    var evts = document.querySelectorAll('div.event');
    for (var i = 0; i < evts.length; i++) {
        var rect = evts[i].getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0) {
            evts[i].click();
            return {
                clicked: true, 
                index: i,
                width: rect.width, 
                style: evts[i].getAttribute('style'),
                parentClass: evts[i].parentElement.className
            };
        }
    }
    return {clicked: false, total: evts.length};
""")
print(json.dumps(click_result, indent=2))

time.sleep(2)

# Now check if the booking details popup appeared
print("\n=== Checking for booking details popup ===")
try:
    details = d.execute_script("""
        if (typeof vems !== 'undefined' && vems.bookingDetailsVM) {
            var vm = vems.bookingDetailsVM;
            var details = vm.details ? vm.details() : null;
            if (details && details.length > 0) {
                return details.map(function(d) {
                    return {field: d.Field, value: d.Value};
                });
            }
            return {keys: Object.keys(vm).slice(0, 20)};
        }
        return {error: 'bookingDetailsVM not found'};
    """)
    print(json.dumps(details, indent=2))
except Exception as e:
    print(f"Error: {e}")

# Also check popup text
print("\n=== Popup text ===")
try:
    popup = d.find_element(By.ID, "detailsContainer")
    if popup.is_displayed():
        print(popup.text)
    else:
        print("detailsContainer exists but not visible")
except:
    print("detailsContainer not found")

# Check modal
try:
    modal = d.find_element(By.CSS_SELECTOR, ".modal.in, .modal.show, .modal[style*='display: block']")
    print(f"\nModal visible: {modal.is_displayed()}")
    print(f"Modal text: {modal.text[:500]}")
except:
    print("No visible modal found")

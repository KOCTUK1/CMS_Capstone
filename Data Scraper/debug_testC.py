from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

# Click Search button
print("Clicking Search...")
for s in d.find_elements(By.CSS_SELECTOR, "button.find-a-room"):
    if s.is_displayed():
        s.click()
        print("  Clicked!")
        break

# Wait longer for grid to fully load
print("Waiting 10 seconds for grid to load...")
time.sleep(10)

# Now check for event-items
src = d.page_source
print(f"\nevent-item count in source: {src.count('event-item')}")
print(f"booking-grid count: {src.count('booking-grid')}")

# Try to find event-item elements with Selenium
for selector in [".event-item", "div.event-item", "[class*='event-item']",
                  ".event-item-name", ".event-item-time", ".event-item-location"]:
    elements = d.find_elements(By.CSS_SELECTOR, selector)
    if elements:
        print(f"\n{selector}: {len(elements)} elements found")
        for i, el in enumerate(elements[:5]):
            txt = el.text.strip() if el.text else "(no text)"
            cls = el.get_attribute("class") or ""
            tag = el.tag_name
            vis = el.is_displayed()
            w = el.size.get("width", 0)
            h = el.size.get("height", 0)
            print(f"  [{i}] tag={tag} class={cls} visible={vis} size={w}x{h} text={txt[:80]}")

# Check if it's inside an Angular/React shadow DOM
print("\n=== Checking for Shadow DOM ===")
shadow_hosts = d.execute_script("""
    var all = document.querySelectorAll('*');
    var hosts = [];
    for (var i = 0; i < all.length; i++) {
        if (all[i].shadowRoot) hosts.push(all[i].tagName + '.' + all[i].className);
    }
    return hosts;
""")
print(f"Shadow DOM hosts: {shadow_hosts}")

# Try to get event items via JavaScript
print("\n=== JS Query for event items ===")
js_result = d.execute_script("""
    var items = document.querySelectorAll('[class*="event-item"]');
    var results = [];
    for (var i = 0; i < Math.min(items.length, 10); i++) {
        results.push({
            tag: items[i].tagName,
            cls: items[i].className,
            text: items[i].innerText.substring(0, 100),
            visible: items[i].offsetParent !== null,
            rect: items[i].getBoundingClientRect()
        });
    }
    return results;
""")
for i, item in enumerate(js_result):
    print(f"  [{i}] tag={item['tag']} class={item['cls']}")
    print(f"       text={item['text'][:80]}")
    print(f"       visible={item['visible']} rect=({item['rect']['x']:.0f},{item['rect']['y']:.0f} {item['rect']['width']:.0f}x{item['rect']['height']:.0f})")

# Also check: what does the schedule area look like now?
print("\n=== Visible body text around schedule ===")
body = d.find_element(By.TAG_NAME, "body").text
if "Rooms You Can Request" in body:
    idx = body.find("Rooms You Can Request")
    print(body[idx:idx+1000])

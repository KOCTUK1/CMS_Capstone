from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

# Click Search
print("Clicking Search...")
for s in d.find_elements(By.CSS_SELECTOR, "button.find-a-room"):
    if s.is_displayed():
        s.click()
        break

# Wait for full render
print("Waiting 12 seconds...")
time.sleep(12)

# Get ALL absolutely positioned colored elements (not just the building-hours ones)
print("\n=== ALL positioned colored elements (excluding building-hours gray) ===")
elements = d.execute_script("""
    var all = document.querySelectorAll('div, span, a');
    var found = [];
    for (var i = 0; i < all.length; i++) {
        var style = window.getComputedStyle(all[i]);
        var bg = style.backgroundColor;
        if (style.position === 'absolute' && bg && 
            bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent' &&
            bg !== 'rgb(255, 255, 255)' && bg !== 'rgb(0, 0, 0)' &&
            bg !== 'rgb(232, 233, 235)') {
            var rect = all[i].getBoundingClientRect();
            if (rect.width > 3 && rect.height > 3) {
                found.push({
                    tag: all[i].tagName,
                    cls: all[i].className.substring(0, 100),
                    bg: bg,
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    w: Math.round(rect.width),
                    h: Math.round(rect.height),
                    text: all[i].innerText.substring(0, 80),
                    html: all[i].outerHTML.substring(0, 200),
                    clickable: all[i].onclick !== null || all[i].tagName === 'A',
                    databind: all[i].getAttribute('data-bind') ? all[i].getAttribute('data-bind').substring(0, 100) : ''
                });
            }
        }
    }
    return found;
""")
print(f"Found {len(elements)} elements\n")
for el in elements:
    print(f"  {el['tag']} bg={el['bg']} pos=({el['x']},{el['y']}) size={el['w']}x{el['h']}")
    print(f"    class={el['cls']}")
    print(f"    text={el['text'][:60]}")
    if el['databind']:
        print(f"    data-bind={el['databind']}")
    print(f"    html={el['html'][:150]}")
    print()

# Also search for ANY blue-ish colored elements regardless of position
print("\n=== Blue-ish elements (any position) ===")
blue_els = d.execute_script("""
    var all = document.querySelectorAll('*');
    var found = [];
    for (var i = 0; i < all.length; i++) {
        var style = window.getComputedStyle(all[i]);
        var bg = style.backgroundColor;
        // Check for blue-ish colors
        if (bg && bg.match(/rgb\\((\\d+),\\s*(\\d+),\\s*(\\d+)\\)/)) {
            var m = bg.match(/rgb\\((\\d+),\\s*(\\d+),\\s*(\\d+)\\)/);
            var r = parseInt(m[1]), g = parseInt(m[2]), b = parseInt(m[3]);
            // Blue if b > r and b > g
            if (b > 150 && b > r && b > g) {
                var rect = all[i].getBoundingClientRect();
                found.push({
                    tag: all[i].tagName,
                    cls: all[i].className.substring(0, 80),
                    bg: bg,
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    w: Math.round(rect.width),
                    h: Math.round(rect.height),
                    text: all[i].innerText.substring(0, 60)
                });
            }
        }
    }
    return found.slice(0, 30);
""")
print(f"Found {len(blue_els)} blue elements")
for el in blue_els:
    print(f"  {el['tag']} bg={el['bg']} pos=({el['x']},{el['y']}) size={el['w']}x{el['h']} class={el['cls']} text={el['text'][:40]}")

# Check for any Knockout-rendered booking elements
print("\n=== Knockout-rendered elements with data-bind containing 'event' or 'book' ===")
ko_els = d.execute_script("""
    var all = document.querySelectorAll('[data-bind]');
    var found = [];
    for (var i = 0; i < all.length; i++) {
        var db = all[i].getAttribute('data-bind');
        if (db && (db.toLowerCase().includes('event') || db.toLowerCase().includes('book'))) {
            found.push({
                tag: all[i].tagName,
                cls: all[i].className.substring(0, 80),
                databind: db.substring(0, 150),
                text: all[i].innerText.substring(0, 60),
                childCount: all[i].children.length
            });
        }
    }
    return found.slice(0, 20);
""")
print(f"Found {len(ko_els)} elements")
for el in ko_els:
    print(f"  {el['tag']} class={el['cls']} children={el['childCount']}")
    print(f"    data-bind: {el['databind']}")
    print(f"    text: {el['text'][:60]}")
    print()

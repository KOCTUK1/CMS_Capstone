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
        print("  Clicked!")
        break

# Poll for booking data to load - check every 2 seconds for 30 seconds
print("Polling for booking blocks to load (up to 30 seconds)...")
for i in range(15):
    time.sleep(2)
    
    # Count event-item elements via JS
    count = d.execute_script("return document.querySelectorAll('[class*=\"event-item\"]').length")
    
    # Also check for any elements inside the booking-grid
    grid_children = d.execute_script("""
        var grids = document.querySelectorAll('[class*="booking-grid"]');
        var total = 0;
        for (var g = 0; g < grids.length; g++) {
            total += grids[g].children.length;
        }
        return total;
    """)
    
    # Check for colored divs with position absolute (typical for timeline blocks)
    abs_colored = d.execute_script("""
        var all = document.querySelectorAll('div, span, a');
        var found = 0;
        for (var i = 0; i < all.length; i++) {
            var style = window.getComputedStyle(all[i]);
            if (style.position === 'absolute' && 
                style.backgroundColor && 
                style.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
                style.backgroundColor !== 'transparent' &&
                style.backgroundColor !== 'rgb(255, 255, 255)') {
                found++;
            }
        }
        return found;
    """)
    
    print(f"  {(i+1)*2}s: event-items={count}, grid-children={grid_children}, abs-colored={abs_colored}")
    
    if count > 6 or grid_children > 5:
        print("  Booking data detected!")
        break

# Now let's look at what's inside the booking-grid
print("\n=== booking-grid contents ===")
grid_info = d.execute_script("""
    var grids = document.querySelectorAll('[class*="booking-grid"]');
    var results = [];
    for (var g = 0; g < grids.length; g++) {
        var grid = grids[g];
        var info = {
            class: grid.className,
            childCount: grid.children.length,
            innerHTML: grid.innerHTML.substring(0, 500),
            children: []
        };
        for (var c = 0; c < Math.min(grid.children.length, 5); c++) {
            info.children.push({
                tag: grid.children[c].tagName,
                class: grid.children[c].className,
                text: grid.children[c].innerText.substring(0, 100)
            });
        }
        results.push(info);
    }
    return results;
""")
for i, g in enumerate(grid_info):
    print(f"\nGrid {i}: class={g['class']} children={g['childCount']}")
    print(f"  innerHTML preview: {g['innerHTML'][:300]}")
    for c in g['children']:
        print(f"  child: tag={c['tag']} class={c['class']} text={c['text'][:80]}")

# Look for absolutely positioned elements that could be booking blocks
print("\n=== Absolutely positioned colored elements ===")
abs_elements = d.execute_script("""
    var all = document.querySelectorAll('div, span, a');
    var found = [];
    for (var i = 0; i < all.length; i++) {
        var style = window.getComputedStyle(all[i]);
        if (style.position === 'absolute' && 
            style.backgroundColor && 
            style.backgroundColor !== 'rgba(0, 0, 0, 0)' &&
            style.backgroundColor !== 'transparent' &&
            style.backgroundColor !== 'rgb(255, 255, 255)' &&
            style.backgroundColor !== 'rgb(0, 0, 0)') {
            var rect = all[i].getBoundingClientRect();
            if (rect.width > 5 && rect.height > 5) {
                found.push({
                    tag: all[i].tagName,
                    cls: all[i].className.substring(0, 80),
                    bg: style.backgroundColor,
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    w: Math.round(rect.width),
                    h: Math.round(rect.height),
                    text: all[i].innerText.substring(0, 50)
                });
            }
        }
    }
    return found.slice(0, 20);
""")
for el in abs_elements:
    print(f"  {el['tag']} class={el['cls']} bg={el['bg']} pos=({el['x']},{el['y']}) size={el['w']}x{el['h']} text={el['text'][:50]}")

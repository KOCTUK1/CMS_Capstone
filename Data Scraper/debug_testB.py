from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

# Step 1: Click "Let Me Search For A Room"
print("Step 1: Clicking 'Let Me Search For A Room'...")
links = d.find_elements(By.XPATH, "//*[contains(text(),'Let Me Search For A Room')]")
for link in links:
    if link.is_displayed():
        print(f"  Found it! Tag: {link.tag_name}, clicking...")
        link.click()
        time.sleep(3)
        break
else:
    print("  NOT FOUND")

# Step 2: Look for Schedule tab
print("\nStep 2: Looking for Schedule tab...")
links = d.find_elements(By.XPATH, "//*[contains(text(),'Schedule')]")
for link in links:
    if link.is_displayed():
        tag = link.tag_name
        cls = link.get_attribute("class") or ""
        txt = link.text.strip()
        print(f"  Found: tag={tag} class={cls} text={txt}")

# Step 3: Click Search
print("\nStep 3: Looking for Search buttons...")
searches = d.find_elements(By.XPATH, "//*[contains(text(),'Search')]")
for s in searches:
    if s.is_displayed():
        tag = s.tag_name
        cls = s.get_attribute("class") or ""
        txt = s.text.strip()
        print(f"  Found: tag={tag} class={cls} text={txt}")

# Click the first visible Search that looks like a button
print("\nStep 4: Clicking Search...")
for s in searches:
    if s.is_displayed():
        txt = s.text.strip()
        tag = s.tag_name
        if txt == "Search" and tag in ["a", "button", "input"]:
            print(f"  Clicking: tag={tag} text={txt}")
            s.click()
            time.sleep(5)
            break

# Step 5: Check what loaded
print("\nStep 5: Checking results...")
src = d.page_source
print("event-item count:", src.count("event-item"))
print("booking-grid count:", src.count("booking-grid"))
print("Rooms You Can Request:", "Rooms You Can Request" in src)
print("still says 'will appear here':", "will appear here" in src)

# Print the visible text now
body = d.find_element(By.TAG_NAME, "body").text
if "Rooms You Can Request" in body:
    idx = body.find("Rooms You Can Request")
    print("\n=== SCHEDULE AREA (500 chars) ===")
    print(body[idx:idx+500])
else:
    print("\n=== LAST 500 chars of visible text ===")
    print(body[-500:])

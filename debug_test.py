from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time

o = Options()
o.debugger_address = "127.0.0.1:9222"
d = webdriver.Edge(options=o)

d.get("https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz")
time.sleep(5)

print("=== INPUT FIELDS ===")
for inp in d.find_elements(By.CSS_SELECTOR, "input"):
    name = inp.get_attribute("name") or ""
    val = inp.get_attribute("value") or ""
    typ = inp.get_attribute("type") or ""
    if val:
        print(f"  name={name} type={typ} value={val}")

print()
print("=== PAGE SOURCE SEARCH ===")
src = d.page_source
print("event-item count:", src.count("event-item"))
print("booking-grid count:", src.count("booking-grid"))
print("Rooms You Can Request:", "Rooms You Can Request" in src)
print("Schedule tab:", "Schedule" in src)
print("Let Me Search:", "Let Me Search" in src)

print()
print("=== VISIBLE TEXT (first 2000 chars) ===")
print(d.find_element(By.TAG_NAME, "body").text[:2000])

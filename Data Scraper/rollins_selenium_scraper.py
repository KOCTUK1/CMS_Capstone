"""
Rollins EMS Scraper - Connects to your already-logged-in Edge browser.

Setup:
    pip install selenium pandas

Before running:
    1. Close Edge completely: taskkill /F /IM msedge.exe
    2. Open Edge with debug port (in PowerShell):
       & "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222
    3. Log into EMS through Okta in that Edge window
    4. Run this script in a SEPARATE PowerShell window

Usage:
    python rollins_selenium_scraper.py
"""

import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
import re
import logging

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    ElementClickInterceptedException, StaleElementReferenceException
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

EMS_URL = "https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz"

TARGET_BUILDINGS = [
    "Bush Science Center",
    "Kathleen W Rollins Hall",
    "Olin Library",
]

ROOM_CAPACITIES = {
    "Bush Auditorium and Lobby": 345,
    "Room 123": 12, "Room 176": 46, "Room 200": 20, "Room 201": 44,
    "Room 202": 22, "Room 208": 22, "Room 210": 32, "Room 212": 24,
    "Room 228": 24, "Room 260": 13, "Room 277": 14, "Room 301": 32,
    "Room 302": 24, "Room 308": 30, "Room 310": 20,
    "Mills Lawn": 1000, "Room 128": 26, "Room 300": 150,
    "Ourisman": 4, "Genius": 36, "Hauske": 36,
    "Room 330": 20, "Room 340": 20, "Tars Plaza": 50,
    "Room 104": 16, "Room 220": 25, "Room 225": 35,
    "Room 230": 30, "Room 319": 20, "Van Houten": 15,
}


def get_capacity(room_name):
    for key, cap in ROOM_CAPACITIES.items():
        if key.lower() in room_name.lower():
            return cap
    return None


def get_building(location_text):
    for building in TARGET_BUILDINGS:
        if building.lower() in location_text.lower():
            return building
    return "Unknown"


def get_room(location_text):
    if " - " in location_text:
        parts = location_text.split(" - ", 1)
        return parts[1].strip() if len(parts) > 1 else location_text
    return location_text


def connect_to_edge():
    options = Options()
    options.debugger_address = "127.0.0.1:9222"
    driver = webdriver.Edge(options=options)
    log.info(f"Connected to Edge. Current URL: {driver.current_url}")
    return driver


def convert_to_24h(time_str):
    try:
        return datetime.strptime(time_str.strip(), "%I:%M %p").strftime("%H:%M")
    except ValueError:
        try:
            return datetime.strptime(time_str.strip(), "%I:%M%p").strftime("%H:%M")
        except ValueError:
            return time_str


def navigate_to_date(driver, date):
    """Navigate to the schedule view for a specific date."""
    try:
        date_str = date.strftime("%m/%d/%Y")

        # Load the room request page
        driver.get(EMS_URL)
        time.sleep(4)

        # Step 1: Change the date
        date_set = False
        inputs = driver.find_elements(By.CSS_SELECTOR, "input")
        for inp in inputs:
            try:
                val = inp.get_attribute("value") or ""
                if re.match(r"\d{2}/\d{2}/\d{4}", val):
                    # Triple-click to select all, then type new date
                    inp.click()
                    time.sleep(0.2)
                    inp.send_keys(Keys.CONTROL + "a")
                    time.sleep(0.1)
                    inp.send_keys(date_str)
                    time.sleep(0.2)
                    inp.send_keys(Keys.TAB)  # Tab out to trigger change
                    date_set = True
                    log.info(f"  Set date to {date_str}")
                    break
            except (StaleElementReferenceException, Exception):
                continue

        if not date_set:
            log.warning(f"  Could not set date for {date_str}")

        time.sleep(2)

        # Step 2: Click "Let Me Search For A Room"
        try:
            search_link = driver.find_element(By.XPATH,
                "//a[contains(text(),'Let Me Search For A Room')] | "
                "//button[contains(text(),'Let Me Search For A Room')]")
            if search_link.is_displayed():
                search_link.click()
                log.info("  Clicked 'Let Me Search For A Room'")
                time.sleep(2)
        except NoSuchElementException:
            log.info("  'Let Me Search For A Room' not found (may already be in search mode)")

        # Step 3: Click "Schedule" tab if visible
        try:
            for link in driver.find_elements(By.XPATH, "//a[contains(text(),'Schedule')]"):
                if link.is_displayed():
                    link.click()
                    log.info("  Clicked 'Schedule' tab")
                    time.sleep(2)
                    break
        except Exception:
            pass

        # Step 4: Click "Search" button to load results
        try:
            search_buttons = driver.find_elements(By.XPATH,
                "//a[text()='Search'] | "
                "//button[text()='Search'] | "
                "//input[@value='Search'] | "
                "//a[contains(@class,'search')] | "
                "//button[contains(@class,'search')]")
            for btn in search_buttons:
                if btn.is_displayed():
                    btn.click()
                    log.info("  Clicked 'Search'")
                    time.sleep(4)
                    break
        except Exception:
            pass

        # Step 5: Wait for the booking grid to load
        # Look for signs that the schedule has loaded
        for _ in range(10):
            try:
                source = driver.page_source
                if "event-item" in source or "booking-grid" in source or "Rooms You Can Request" in source:
                    log.info(f"  Schedule loaded for {date.strftime('%Y-%m-%d')}")
                    time.sleep(2)
                    return True
            except Exception:
                pass
            time.sleep(1)

        log.info(f"  Page loaded for {date.strftime('%Y-%m-%d')} (may not have schedule)")
        return True

    except Exception as e:
        log.error(f"  Navigation error: {e}")
        return False


def extract_bookings(driver, date):
    """Find booking blocks using known CSS classes and click each one."""
    records = []
    time.sleep(2)

    # From previous debug: event-item, event-item-time, event-item-name,
    # event-item-location, event-item-color, booking-grid, drawer-eventname
    booking_selectors = [
        ".event-item",
        "div.event-item",
        "a.event-item",
        "span.event-item",
        "[class*='event-item']",
        ".booking-grid .event-item",
        ".event-item-color",
    ]

    booking_elements = []
    for selector in booking_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                log.info(f"    Found {len(elements)} elements with: {selector}")
                booking_elements = elements
                break
        except Exception:
            continue

    # Try JavaScript if CSS selectors fail
    if not booking_elements:
        try:
            count = driver.execute_script(
                "return document.querySelectorAll('.event-item, [class*=\"event-item\"]').length;")
            log.info(f"    JS count of event-item elements: {count}")

            if count > 0:
                # Get them via JS and click via index
                booking_elements = driver.execute_script("""
                    var items = document.querySelectorAll('.event-item');
                    return Array.from(items);
                """) or []
        except Exception:
            pass

    # Try inside iframes
    if not booking_elements:
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                try:
                    driver.switch_to.frame(iframe)
                    elements = driver.find_elements(By.CSS_SELECTOR, ".event-item")
                    if elements:
                        log.info(f"    Found {len(elements)} event-items inside iframe!")
                        booking_elements = elements
                        break
                    driver.switch_to.default_content()
                except Exception:
                    driver.switch_to.default_content()
        except Exception:
            pass

    if not booking_elements:
        # Debug info
        try:
            source = driver.page_source
            event_count = source.count("event-item")
            booking_count = source.count("booking-grid")
            rooms_text = "Rooms You Can Request" in source
            log.info(f"    DEBUG: event-item={event_count}x, booking-grid={booking_count}x, has_rooms={rooms_text}")

            if event_count > 0:
                # The elements exist in HTML but Selenium can't find them
                # Try extracting data directly from HTML
                log.info("    Attempting direct HTML parsing...")
                html_records = parse_bookings_from_html(source, date)
                if html_records:
                    log.info(f"    HTML parsing found {len(html_records)} bookings!")
                    return html_records
        except Exception:
            pass

        log.warning("    No booking blocks found.")
        return records

    # Deduplicate
    seen = set()
    unique = []
    for el in booking_elements:
        try:
            eid = el.id or str(id(el))
            if eid not in seen:
                seen.add(eid)
                unique.append(el)
        except StaleElementReferenceException:
            continue

    log.info(f"    Clicking {len(unique)} blocks...")

    for i, element in enumerate(unique):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.3)

            try:
                element.click()
            except ElementClickInterceptedException:
                driver.execute_script("arguments[0].click();", element)

            time.sleep(1.5)

            record = read_booking_popup(driver, date)
            if record:
                building = record.get("building", "")
                if any(b.lower() in building.lower() for b in TARGET_BUILDINGS):
                    records.append(record)
                    log.info(f"      [{i+1}] {record['building']} - {record['room_name']} "
                             f"({record['start_time']}-{record['end_time']})")

            close_popup(driver)
            time.sleep(0.3)
        except StaleElementReferenceException:
            continue
        except Exception as e:
            log.warning(f"      Error on block {i+1}: {e}")
            close_popup(driver)
            continue

    # Switch back to default content in case we were in an iframe
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

    return records


def parse_bookings_from_html(source, date):
    """Fallback: parse booking data directly from page source HTML."""
    records = []
    try:
        # Look for event-item blocks in the HTML
        # Pattern: <div class="event-item">...<span class="event-item-name">NAME</span>
        #          <span class="event-item-time">TIME</span>
        #          <span class="event-item-location">LOCATION</span>

        # Find all event-item blocks
        pattern = re.compile(
            r'class="[^"]*event-item[^"]*"[^>]*>(.*?)</div>',
            re.DOTALL | re.IGNORECASE
        )

        for match in pattern.finditer(source):
            block = match.group(1)

            name_match = re.search(r'event-item-name[^>]*>([^<]+)', block, re.IGNORECASE)
            time_match = re.search(r'event-item-time[^>]*>([^<]+)', block, re.IGNORECASE)
            loc_match = re.search(r'event-item-location[^>]*>([^<]+)', block, re.IGNORECASE)

            if not time_match or not loc_match:
                continue

            location = loc_match.group(1).strip()

            # Check if it's a target building
            if not any(b.lower() in location.lower() for b in TARGET_BUILDINGS):
                continue

            time_text = time_match.group(1).strip()
            time_range = re.search(
                r'(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-\u2013]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))',
                time_text, re.IGNORECASE)

            if not time_range:
                continue

            event_name = name_match.group(1).strip() if name_match else ""
            start_24 = convert_to_24h(time_range.group(1))
            end_24 = convert_to_24h(time_range.group(2))
            building = get_building(location)
            room_name = get_room(location)
            capacity = get_capacity(room_name)

            try:
                s = datetime.strptime(start_24, "%H:%M")
                e = datetime.strptime(end_24, "%H:%M")
                duration = (e - s).seconds / 3600
            except ValueError:
                duration = None

            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "day_of_week": date.strftime("%A"),
                "event_name": event_name,
                "start_time": start_24,
                "end_time": end_24,
                "duration_hours": round(duration, 2) if duration else None,
                "room_name": room_name,
                "building": building,
                "capacity": capacity,
                "hour_of_day": int(start_24.split(":")[0]) if start_24 else None,
                "month": date.month,
                "week_of_year": date.isocalendar()[1],
                "is_weekend": date.weekday() >= 5,
            })

    except Exception as e:
        log.warning(f"    HTML parse error: {e}")

    return records


def read_booking_popup(driver, date):
    """Read the popup that appears after clicking a booking block."""
    try:
        popup_selectors = [
            "div.popover", "div.popover-content", "div.modal-content",
            "div.popup", "div.tooltip-inner", "div.drawer",
            "div[class*='popover']", "div[class*='detail']",
            "div[class*='popup']", "div[class*='drawer']",
        ]

        popup_text = ""
        for selector in popup_selectors:
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, selector):
                    if el.is_displayed() and el.text.strip():
                        txt = el.text.strip()
                        if re.search(r'\d{1,2}:\d{2}\s*(?:AM|PM)', txt, re.IGNORECASE):
                            popup_text = txt
                            break
                if popup_text:
                    break
            except Exception:
                continue

        # Also try reading fields by their specific class names
        if not popup_text:
            try:
                parts = []
                for cls in [".drawer-eventname", ".event-item-name",
                            ".event-item-time", ".event-date",
                            ".event-item-location"]:
                    for el in driver.find_elements(By.CSS_SELECTOR, cls):
                        if el.is_displayed() and el.text.strip():
                            parts.append(el.text.strip())
                if parts:
                    popup_text = "\n".join(parts)
            except Exception:
                pass

        if not popup_text:
            return None

        event_name = ""
        event_time_start = ""
        event_time_end = ""
        location = ""

        lines = popup_text.split("\n")
        for j, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            if "event name" in line.lower():
                event_name = line.split(":", 1)[-1].strip() if ":" in line else ""
                if not event_name and j + 1 < len(lines):
                    event_name = lines[j + 1].strip()
            time_match = re.search(
                r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-\u2013]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))",
                line, re.IGNORECASE)
            if time_match:
                event_time_start = time_match.group(1).strip()
                event_time_end = time_match.group(2).strip()
            if "location" in line.lower():
                location = line.split(":", 1)[-1].strip() if ":" in line else ""
                if not location and j + 1 < len(lines):
                    location = lines[j + 1].strip()

        if not event_time_start:
            time_match = re.search(
                r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-\u2013]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))",
                popup_text, re.IGNORECASE)
            if time_match:
                event_time_start = time_match.group(1).strip()
                event_time_end = time_match.group(2).strip()

        if not location:
            for building in TARGET_BUILDINGS:
                if building.lower() in popup_text.lower():
                    loc_match = re.search(
                        rf"({re.escape(building)}\s*[-\u2013]\s*[^\n]+)",
                        popup_text, re.IGNORECASE)
                    location = loc_match.group(1).strip() if loc_match else building
                    break

        if not event_time_start or not location:
            return None

        start_24 = convert_to_24h(event_time_start)
        end_24 = convert_to_24h(event_time_end)
        building = get_building(location)
        room_name = get_room(location)
        capacity = get_capacity(room_name)

        try:
            s = datetime.strptime(start_24, "%H:%M")
            e = datetime.strptime(end_24, "%H:%M")
            duration = (e - s).seconds / 3600
        except ValueError:
            duration = None

        return {
            "date": date.strftime("%Y-%m-%d"),
            "day_of_week": date.strftime("%A"),
            "event_name": event_name,
            "start_time": start_24,
            "end_time": end_24,
            "duration_hours": round(duration, 2) if duration else None,
            "room_name": room_name,
            "building": building,
            "capacity": capacity,
            "hour_of_day": int(start_24.split(":")[0]) if start_24 else None,
            "month": date.month,
            "week_of_year": date.isocalendar()[1],
            "is_weekend": date.weekday() >= 5,
        }
    except Exception as e:
        log.warning(f"      Popup read error: {e}")
        return None


def close_popup(driver):
    try:
        for by, sel in [
            (By.CSS_SELECTOR, "button.close"),
            (By.CSS_SELECTOR, "button[aria-label='Close']"),
            (By.XPATH, "//button[contains(text(),'Close')]"),
            (By.XPATH, "//button[contains(text(),'X')]"),
        ]:
            try:
                btn = driver.find_element(by, sel)
                if btn.is_displayed():
                    btn.click()
                    return
            except NoSuchElementException:
                continue
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-03-07")
    parser.add_argument("--end", default="2026-04-30")
    parser.add_argument("--output", default="reservations.csv")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    print(f"\n Rollins EMS Scraper (Edge remote debug)")
    print(f"   Buildings  : {', '.join(TARGET_BUILDINGS)}")
    print(f"   Date range : {start_date.date()} -> {end_date.date()}")
    print(f"   Output     : {args.output}")
    print(f"   Connecting to your already-logged-in Edge...\n")

    driver = connect_to_edge()

    try:
        # Verify logged in
        driver.get(EMS_URL)
        time.sleep(5)
        page = driver.page_source.lower()
        if any(kw in page for kw in ["create a reservation", "my cart", "spielmann"]):
            log.info("Confirmed: logged into EMS!")
        else:
            log.warning("May not be logged in. Check the Edge window.")
            time.sleep(3)

        all_records = []
        current = start_date
        total_days = (end_date - start_date).days + 1
        day_count = 0

        while current <= end_date:
            day_count += 1
            log.info(f"[Day {day_count}/{total_days}] {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})")

            if navigate_to_date(driver, current):
                records = extract_bookings(driver, current)
                all_records.extend(records)
                log.info(f"  -> {len(records)} bookings in target buildings")

            current += timedelta(days=1)
            time.sleep(1)

        if all_records:
            df = pd.DataFrame(all_records)
            df = df.drop_duplicates()
            df = df.sort_values(["date", "building", "room_name", "start_time"]).reset_index(drop=True)
            df.to_csv(args.output, index=False)

            print(f"\n{'='*60}")
            print(f"RESULTS")
            print(f"{'='*60}")
            print(f"Total bookings : {len(df)}")
            print(f"Date range     : {df['date'].min()} -> {df['date'].max()}")
            print(f"Buildings      : {df['building'].nunique()}")
            print(f"Unique rooms   : {df['room_name'].nunique()}")
            print(f"\nBy building:\n{df['building'].value_counts().to_string()}")
            print(f"\nBy day:\n{df['day_of_week'].value_counts().to_string()}")
            print(f"\nBy hour:\n{df['hour_of_day'].value_counts().sort_index().to_string()}")
            print(f"\nSaved to: {args.output}")
            print(f"{'='*60}")
        else:
            print("\nNo bookings collected. See log above.")

    finally:
        log.info("Done. Your Edge browser is still open.")


if __name__ == "__main__":
    main()

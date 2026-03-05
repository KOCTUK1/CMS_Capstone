"""
Rollins EMS Scraper - WORKING VERSION
======================================
Connects to your already-logged-in Edge browser.
Clicks each div.event block, reads booking details from Knockout VM.

Before running:
    1. Close Edge: taskkill /F /IM msedge.exe
    2. Open Edge with debug port:
       & "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222
    3. Log into EMS through Okta
    4. Navigate to the room request schedule page so you see the blue blocks
    5. Run this script in a separate PowerShell window

Usage:
    python rollins_selenium_scraper.py
    python rollins_selenium_scraper.py --start 2026-03-10 --end 2026-04-30
"""

import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
import re
import json
import logging
import html

from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException
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
    "Room 228": 24, "Room 260": 13, "Room 277": 14,
    "Room 301 - Computer": 32, "Room 302": 24, "Room 308": 30,
    "Room 310 - Computer": 20,
    "Mills Lawn": 1000, "Room 128": 26, "Room 300 - Galloway": 150,
    "Room 301 - Ourisman": 4, "Room 310 - Genius": 36,
    "Room 320": 36, "Room 330": 20, "Room 340": 20, "Tars Plaza": 50,
    "Room 104": 16, "Room 220": 25, "Room 225": 35,
    "Room 230": 30, "Room 319": 20, "Van Houten": 15,
}


def get_capacity(location):
    for key, cap in ROOM_CAPACITIES.items():
        if key.lower() in location.lower():
            return cap
    return None


def get_building(location):
    for b in TARGET_BUILDINGS:
        if b.lower() in location.lower():
            return b
    return location.split(" - ")[0].strip() if " - " in location else "Other"


def get_room(location):
    if " - " in location:
        parts = location.split(" - ", 1)
        return parts[1].strip()
    return location


def convert_to_24h(time_str):
    try:
        return datetime.strptime(time_str.strip(), "%I:%M %p").strftime("%H:%M")
    except ValueError:
        try:
            return datetime.strptime(time_str.strip(), "%I:%M%p").strftime("%H:%M")
        except ValueError:
            return time_str


def connect_to_edge():
    options = Options()
    options.debugger_address = "127.0.0.1:9222"
    driver = webdriver.Edge(options=options)
    log.info(f"Connected to Edge. URL: {driver.current_url}")
    return driver


def navigate_to_date(driver, date):
    """Load the schedule page for a specific date."""
    try:
        date_str = date.strftime("%m/%d/%Y")
        driver.get(EMS_URL)
        time.sleep(4)

        # Change the date field
        inputs = driver.find_elements(By.CSS_SELECTOR, "input")
        for inp in inputs:
            try:
                val = inp.get_attribute("value") or ""
                if re.match(r"\w{3}\s+\d{2}/\d{2}/\d{4}", val) or re.match(r"\d{2}/\d{2}/\d{4}", val):
                    inp.click()
                    time.sleep(0.2)
                    inp.send_keys(Keys.CONTROL + "a")
                    time.sleep(0.1)
                    inp.send_keys(date_str)
                    time.sleep(0.2)
                    inp.send_keys(Keys.TAB)
                    time.sleep(1)
                    break
            except (StaleElementReferenceException, Exception):
                continue

        # Click Search button
        time.sleep(1)
        for btn in driver.find_elements(By.CSS_SELECTOR, "button.find-a-room"):
            if btn.is_displayed():
                btn.click()
                break

        # Wait for the grid to load - poll for div.event elements
        for attempt in range(40):
            time.sleep(2)
            count = driver.execute_script("return document.querySelectorAll('div.event').length;")
            if count > 0:
                log.info(f"  Schedule loaded: {count} event blocks found ({(attempt+1)*2}s)")
                time.sleep(2)  # Extra time for full render
                return True

        # Grid loaded but maybe no events today
        body = driver.find_element(By.TAG_NAME, "body").text
        if "Rooms You Can Request" in body:
            log.info(f"  Schedule loaded but 0 events for {date.strftime('%Y-%m-%d')}")
            return True

        log.warning(f"  Could not load schedule for {date.strftime('%Y-%m-%d')}")
        return False

    except Exception as e:
        log.error(f"  Navigation error: {e}")
        return False


def extract_bookings(driver, date):
    """Click each div.event, read booking details from Knockout VM."""
    records = []

    # Get total event count
    total = driver.execute_script("return document.querySelectorAll('div.event').length;")
    if total == 0:
        return records

    log.info(f"    Processing {total} event blocks...")

    target_count = 0
    skip_count = 0

    for i in range(total):
        try:
            # Click event block via JS (by index, avoids stale element issues)
            clicked = driver.execute_script(f"""
                var evts = document.querySelectorAll('div.event');
                if (evts.length > {i}) {{
                    evts[{i}].click();
                    return true;
                }}
                return false;
            """)

            if not clicked:
                break

            time.sleep(0.8)

            # Read booking details from Knockout ViewModel
            details = driver.execute_script("""
                if (typeof vems !== 'undefined' && vems.bookingDetailsVM && vems.bookingDetailsVM.details) {
                    var d = vems.bookingDetailsVM.details();
                    if (d && d.length > 0) {
                        return d.map(function(item) {
                            return {field: item.Field, value: item.Value};
                        });
                    }
                }
                return null;
            """)

            if not details:
                close_modal(driver)
                continue

            # Parse the details
            event_name = ""
            event_date = ""
            event_time = ""
            location = ""

            for field in details:
                f = field["field"]
                v = html.unescape(field["value"] or "")
                if f == "Event Name":
                    event_name = v
                elif f == "Date":
                    event_date = v
                elif f == "Event Time":
                    event_time = v
                elif f == "Location":
                    location = v

            # Check if it's a target building
            building = get_building(location)
            is_target = any(b.lower() in location.lower() for b in TARGET_BUILDINGS)

            if is_target and event_time:
                # Parse time
                time_match = re.search(
                    r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-\u2013]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))",
                    event_time, re.IGNORECASE)

                if time_match:
                    start_time = convert_to_24h(time_match.group(1))
                    end_time = convert_to_24h(time_match.group(2))
                    room_name = get_room(location)
                    capacity = get_capacity(location)

                    try:
                        s = datetime.strptime(start_time, "%H:%M")
                        e = datetime.strptime(end_time, "%H:%M")
                        duration = (e - s).seconds / 3600
                    except ValueError:
                        duration = None

                    records.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "day_of_week": date.strftime("%A"),
                        "event_name": event_name,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration_hours": round(duration, 2) if duration else None,
                        "room_name": room_name,
                        "building": building,
                        "capacity": capacity,
                        "hour_of_day": int(start_time.split(":")[0]),
                        "month": date.month,
                        "week_of_year": date.isocalendar()[1],
                        "is_weekend": date.weekday() >= 5,
                    })
                    target_count += 1
            else:
                skip_count += 1

            # Close the modal
            close_modal(driver)
            time.sleep(0.3)

        except Exception as e:
            log.warning(f"      Error on block {i}: {e}")
            close_modal(driver)
            time.sleep(0.3)
            continue

    log.info(f"    Done: {target_count} target bookings, {skip_count} other buildings skipped")
    return records


def close_modal(driver):
    """Close the booking details modal."""
    try:
        driver.execute_script("""
            var closes = document.querySelectorAll('.modal .close, button.close');
            for (var i = 0; i < closes.length; i++) {
                if (closes[i].offsetParent !== null) {
                    closes[i].click();
                    return;
                }
            }
            var btns = document.querySelectorAll('button');
            for (var i = 0; i < btns.length; i++) {
                if (btns[i].textContent.trim() === 'Close' && btns[i].offsetParent !== null) {
                    btns[i].click();
                    return;
                }
            }
        """)
    except Exception:
        try:
            driver.find_element(By.TAG_NAME, "body").click()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-03-10")
    parser.add_argument("--end", default="2026-04-30")
    parser.add_argument("--output", default="reservations.csv")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")
    total_days = (end_date - start_date).days + 1

    print(f"\n Rollins EMS Scraper")
    print(f"   Buildings  : {', '.join(TARGET_BUILDINGS)}")
    print(f"   Date range : {start_date.date()} -> {end_date.date()} ({total_days} days)")
    print(f"   Output     : {args.output}")
    print(f"   Connecting to Edge...\n")

    driver = connect_to_edge()

    all_records = []
    current = start_date
    day_count = 0

    try:
        while current <= end_date:
            day_count += 1
            log.info(f"[Day {day_count}/{total_days}] {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})")

            if navigate_to_date(driver, current):
                records = extract_bookings(driver, current)
                all_records.extend(records)
                log.info(f"  -> {len(records)} target bookings collected")

            current += timedelta(days=1)

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
            print("\nNo target bookings found.")

    finally:
        log.info("Done. Edge is still open.")


if __name__ == "__main__":
    main()

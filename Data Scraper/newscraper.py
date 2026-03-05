"""
Rollins College EMS - Selenium Room Reservation Scraper
========================================================
Uses a real browser to scrape booking data from the EMS schedule view.
Clicks on each booking block and reads the popup details.

Target buildings:
    - Olin Library
    - Bush Science Center
    - Kathleen W Rollins Hall

Setup:
    pip install selenium pandas webdriver-manager

Usage:
    python rollins_selenium_scraper.py --username YOURNETID --password 'YOURPASS'
    python rollins_selenium_scraper.py --username YOURNETID --password 'YOURPASS' --start 2026-03-07 --end 2026-04-30
    python rollins_selenium_scraper.py --username YOURNETID --password 'YOURPASS' --output my_data.csv

Notes:
    - A Chrome browser window will open and you'll see it navigating automatically.
    - Don't touch the browser while it's running.
    - It takes ~30-60 seconds per day depending on how many bookings there are.
    - For 55 days (Mar 7 - Apr 30), expect ~30-55 minutes total.
"""

import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
import re
import logging

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    ElementClickInterceptedException, StaleElementReferenceException
)

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except ImportError:
    USE_WDM = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

EMS_URL = "https://rollins.emscloudservice.com/web/RoomRequest.aspx?data=ity3Dem%2byxxGFZTQvNr97yLU%2byk57qiz"

TARGET_BUILDINGS = [
    "Bush Science Center",
    "Kathleen W Rollins Hall",
    "Olin Library",
]

# Room lookup for capacity data
ROOM_CAPACITIES = {
    # Bush Science Center
    "Bush Auditorium and Lobby": 345,
    "Room 123": 12, "Room 176": 46, "Room 200": 20, "Room 201": 44,
    "Room 202": 22, "Room 208": 22, "Room 210": 32, "Room 212": 24,
    "Room 228": 24, "Room 260": 13, "Room 277": 14, "Room 301": 32,
    "Room 302": 24, "Room 308": 30, "Room 310": 20,
    # Kathleen W Rollins Hall
    "Mills Lawn": 1000, "Room 128": 26, "Room 300": 150,
    "Room 301 - Ourisman": 4, "Room 310 - Genius": 36,
    "Room 320": 36, "Room 330": 20, "Room 340": 20, "Tars Plaza": 50,
    # Olin Library
    "Room 104": 16, "Room 220": 25, "Room 225": 35,
    "Room 230": 30, "Room 319": 20, "Van Houten": 15,
}


def get_capacity(room_name: str) -> int | None:
    """Look up room capacity by partial name match."""
    for key, cap in ROOM_CAPACITIES.items():
        if key.lower() in room_name.lower():
            return cap
    return None


def get_building(location_text: str) -> str:
    """Extract building name from location string like 'Bush Science Center - Room 200'."""
    for building in TARGET_BUILDINGS:
        if building.lower() in location_text.lower():
            return building
    return "Unknown"


def get_room(location_text: str) -> str:
    """Extract room name from location string."""
    if " - " in location_text:
        parts = location_text.split(" - ", 1)
        return parts[1].strip() if len(parts) > 1 else location_text
    return location_text


# ── Browser Setup ──────────────────────────────────────────────────────────────

def create_driver() -> webdriver.Chrome:
    """Create and configure a Chrome WebDriver."""
    options = Options()
    # Run in visible mode so you can see what's happening
    # options.add_argument("--headless")  # Uncomment to run hidden
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    # Prevent the site from detecting automation
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if USE_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    # Hide webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver


# ── Login ──────────────────────────────────────────────────────────────────────

def login(driver: webdriver.Chrome, username: str, password: str) -> bool:
    """Log in to the Rollins EMS system."""
    try:
        log.info("Navigating to EMS...")
        driver.get("https://rollins.emscloudservice.com/web/Default.aspx")
        time.sleep(3)

        # Look for username/password fields
        # EMS might redirect to a Rollins SSO page
        wait = WebDriverWait(driver, 15)

        # Try common login field patterns
        username_selectors = [
            (By.ID, "UserName"),
            (By.ID, "username"),
            (By.NAME, "UserName"),
            (By.NAME, "username"),
            (By.CSS_SELECTOR, "input[type='text']"),
            (By.CSS_SELECTOR, "input[type='email']"),
        ]

        password_selectors = [
            (By.ID, "Password"),
            (By.ID, "password"),
            (By.NAME, "Password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
        ]

        username_field = None
        for by, selector in username_selectors:
            try:
                username_field = wait.until(EC.presence_of_element_located((by, selector)))
                break
            except TimeoutException:
                continue

        if not username_field:
            log.warning("Could not find username field. Page might use SSO.")
            log.info(f"Current URL: {driver.current_url}")
            log.info("Waiting 30 seconds for manual login...")
            time.sleep(30)
            return True

        username_field.clear()
        username_field.send_keys(username)
        time.sleep(0.5)

        password_field = None
        for by, selector in password_selectors:
            try:
                password_field = driver.find_element(by, selector)
                break
            except NoSuchElementException:
                continue

        if password_field:
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(0.5)

        # Find and click submit button
        submit_selectors = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.CSS_SELECTOR, ".btn-primary"),
            (By.CSS_SELECTOR, "#loginButton"),
            (By.XPATH, "//button[contains(text(),'Log')]"),
            (By.XPATH, "//button[contains(text(),'Sign')]"),
        ]

        for by, selector in submit_selectors:
            try:
                submit = driver.find_element(by, selector)
                submit.click()
                break
            except NoSuchElementException:
                continue

        time.sleep(5)
        log.info(f"After login, URL: {driver.current_url}")

        # Check if we're logged in
        page_text = driver.page_source.lower()
        if "logout" in page_text or "my events" in page_text or username.lower() in page_text:
            log.info("Login successful!")
            return True
        else:
            log.warning("Login may have failed. Continuing anyway...")
            return True  # Continue and see what happens

    except Exception as e:
        log.error(f"Login error: {e}")
        return False


# ── Navigate to Schedule View ──────────────────────────────────────────────────

def navigate_to_schedule(driver: webdriver.Chrome, date: datetime) -> bool:
    """Navigate to the room request schedule view for a specific date."""
    try:
        # Go to the room request page with the specific form template
        driver.get(EMS_URL)
        time.sleep(3)

        wait = WebDriverWait(driver, 15)

        # Find and update the date field
        date_str = date.strftime("%m/%d/%Y")

        date_selectors = [
            (By.CSS_SELECTOR, "input[type='date']"),
            (By.CSS_SELECTOR, "input.date-input"),
            (By.CSS_SELECTOR, "input[data-date]"),
            (By.XPATH, "//input[contains(@class,'date')]"),
            (By.XPATH, "//input[contains(@id,'date')]"),
            (By.XPATH, "//input[contains(@id,'Date')]"),
            (By.XPATH, "//input[contains(@name,'date')]"),
            (By.XPATH, "//input[contains(@name,'Date')]"),
        ]

        date_field = None
        for by, selector in date_selectors:
            try:
                date_field = driver.find_element(by, selector)
                if date_field.is_displayed():
                    break
                date_field = None
            except NoSuchElementException:
                continue

        if date_field:
            date_field.clear()
            date_field.send_keys(date_str)
            time.sleep(1)
            # Click elsewhere to trigger date change
            try:
                driver.find_element(By.TAG_NAME, "body").click()
            except Exception:
                pass
            time.sleep(2)
        else:
            # Try clicking on the displayed date text and using JS
            log.info("  Trying JavaScript date injection...")
            js_date = date.strftime("%m/%d/%Y")
            driver.execute_script(f"""
                var inputs = document.querySelectorAll('input');
                for (var i = 0; i < inputs.length; i++) {{
                    var val = inputs[i].value;
                    if (val && val.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) {{
                        inputs[i].value = '{js_date}';
                        inputs[i].dispatchEvent(new Event('change', {{bubbles: true}}));
                        break;
                    }}
                }}
            """)
            time.sleep(3)

        # Make sure we're on Schedule view (not List view)
        try:
            schedule_tab = driver.find_element(By.XPATH, "//a[contains(text(),'Schedule')]")
            if schedule_tab:
                schedule_tab.click()
                time.sleep(2)
        except NoSuchElementException:
            pass

        # Click "Find A Room" or similar button to load results
        find_selectors = [
            (By.XPATH, "//button[contains(text(),'Find')]"),
            (By.XPATH, "//a[contains(text(),'Find')]"),
            (By.XPATH, "//input[@value='Find']"),
            (By.CSS_SELECTOR, ".btn-search"),
            (By.CSS_SELECTOR, "#btn-find"),
        ]

        for by, selector in find_selectors:
            try:
                btn = driver.find_element(by, selector)
                if btn.is_displayed():
                    btn.click()
                    time.sleep(3)
                    break
            except NoSuchElementException:
                continue

        log.info(f"  Loaded schedule for {date.strftime('%Y-%m-%d')}")
        return True

    except Exception as e:
        log.error(f"  Navigation error for {date.strftime('%Y-%m-%d')}: {e}")
        return False


# ── Extract Booking Data ───────────────────────────────────────────────────────

def extract_bookings(driver: webdriver.Chrome, date: datetime) -> list[dict]:
    """
    Find all blue booking blocks on the schedule grid,
    click each one, and extract the popup details.
    """
    records = []

    try:
        # Wait for the schedule grid to load
        time.sleep(2)

        # Find booking blocks - these are typically colored divs on the timeline
        # EMS uses various CSS classes for booked slots
        booking_selectors = [
            "div.book-grid-event",
            "div.event-block",
            "div.booking-block",
            "div.reserved-block",
            "div[class*='event']",
            "div[class*='booking']",
            "div[class*='reserved']",
            "a[class*='event']",
            "div.fc-event",
            "div.grid-event",
            # EMS Cloud specific
            "div.browse-event",
            "div.schedule-event",
            "div[data-event-id]",
            "div[data-booking-id]",
            "td.booked",
            "td.reserved",
        ]

        booking_elements = []
        for selector in booking_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    log.info(f"    Found {len(elements)} elements with selector: {selector}")
                    booking_elements.extend(elements)
            except Exception:
                continue

        # Deduplicate by element reference
        seen = set()
        unique_elements = []
        for el in booking_elements:
            try:
                el_id = el.id
                if el_id not in seen:
                    seen.add(el_id)
                    unique_elements.append(el)
            except StaleElementReferenceException:
                continue

        if not unique_elements:
            # Fallback: look for any clickable colored elements in the grid area
            log.info("    No booking elements found with known selectors. Trying fallback...")
            try:
                # Find all small colored divs that might be bookings
                all_divs = driver.find_elements(By.CSS_SELECTOR, "div")
                for div in all_divs:
                    try:
                        bg = div.value_of_css_property("background-color")
                        width = div.size.get("width", 0)
                        height = div.size.get("height", 0)
                        # Booking blocks are typically colored, narrow-ish, and within the grid
                        if (bg and bg not in ["rgba(0, 0, 0, 0)", "transparent", "rgba(255, 255, 255, 1)"]
                                and 5 < height < 50 and width > 10):
                            unique_elements.append(div)
                    except StaleElementReferenceException:
                        continue
                if unique_elements:
                    log.info(f"    Fallback found {len(unique_elements)} potential booking blocks")
            except Exception as e:
                log.warning(f"    Fallback search failed: {e}")

        log.info(f"    Processing {len(unique_elements)} booking blocks...")

        for i, element in enumerate(unique_elements):
            try:
                # Scroll element into view
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(0.3)

                # Click the booking block
                try:
                    element.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", element)

                time.sleep(1)

                # Look for the popup/modal with booking details
                popup_record = read_booking_popup(driver, date)
                if popup_record:
                    # Check if it's in our target buildings
                    building = popup_record.get("building", "")
                    if any(b.lower() in building.lower() for b in TARGET_BUILDINGS):
                        records.append(popup_record)
                        log.info(f"      [{i+1}] {popup_record['building']} - {popup_record['room_name']} "
                                 f"({popup_record['start_time']} - {popup_record['end_time']})")

                # Close the popup
                close_popup(driver)
                time.sleep(0.3)

            except StaleElementReferenceException:
                continue
            except Exception as e:
                log.warning(f"      Error on block {i+1}: {e}")
                close_popup(driver)
                continue

    except Exception as e:
        log.error(f"    Extraction error: {e}")

    return records


def read_booking_popup(driver: webdriver.Chrome, date: datetime) -> dict | None:
    """Read booking details from the popup that appears after clicking a block."""
    try:
        wait = WebDriverWait(driver, 5)

        # Look for the popup/modal
        popup_selectors = [
            (By.CSS_SELECTOR, "div.popover"),
            (By.CSS_SELECTOR, "div.modal-content"),
            (By.CSS_SELECTOR, "div.popup"),
            (By.CSS_SELECTOR, "div.tooltip-inner"),
            (By.CSS_SELECTOR, "div[class*='popover']"),
            (By.CSS_SELECTOR, "div[class*='detail']"),
            (By.CSS_SELECTOR, "div[class*='popup']"),
            (By.CSS_SELECTOR, "div[class*='modal']"),
            (By.CSS_SELECTOR, "div.booking-details"),
            (By.CSS_SELECTOR, "div.event-details"),
        ]

        popup = None
        for by, selector in popup_selectors:
            try:
                popup = wait.until(EC.visibility_of_element_located((by, selector)))
                if popup and popup.text.strip():
                    break
                popup = None
            except TimeoutException:
                continue

        if not popup:
            return None

        popup_text = popup.text.strip()

        # Parse the popup text
        # Expected format from user's example:
        # Event Name: Saturday Info Sessions/Experience Rollins
        # Date: Saturday Mar 7, 2026
        # Event Time: 7:30 AM - 12:30 PM
        # Location: Bush Science Center - Bush Auditorium and Lobby

        event_name = ""
        event_time_start = ""
        event_time_end = ""
        location = ""

        # Try to extract fields
        lines = popup_text.split("\n")
        for line in lines:
            line = line.strip()
            if not line:
                continue

            if "event name" in line.lower():
                event_name = line.split(":", 1)[-1].strip() if ":" in line else ""
            elif "event time" in line.lower() or "time" in line.lower():
                time_match = re.search(
                    r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))",
                    line, re.IGNORECASE
                )
                if time_match:
                    event_time_start = time_match.group(1).strip()
                    event_time_end = time_match.group(2).strip()
            elif "location" in line.lower():
                location = line.split(":", 1)[-1].strip() if ":" in line else ""

        # Also try regex on the full text if field parsing missed things
        if not event_time_start:
            time_match = re.search(
                r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}:\d{2}\s*(?:AM|PM))",
                popup_text, re.IGNORECASE
            )
            if time_match:
                event_time_start = time_match.group(1).strip()
                event_time_end = time_match.group(2).strip()

        if not location:
            # Look for building names in the text
            for building in TARGET_BUILDINGS:
                if building.lower() in popup_text.lower():
                    # Try to get the full location line
                    loc_match = re.search(
                        rf"({re.escape(building)}\s*[-–]\s*[^\n]+)",
                        popup_text, re.IGNORECASE
                    )
                    location = loc_match.group(1).strip() if loc_match else building
                    break

        if not event_time_start or not location:
            return None

        # Parse times to 24h format
        start_24 = convert_to_24h(event_time_start)
        end_24 = convert_to_24h(event_time_end)

        building = get_building(location)
        room_name = get_room(location)
        capacity = get_capacity(room_name)

        # Calculate duration
        try:
            start_dt = datetime.strptime(start_24, "%H:%M")
            end_dt = datetime.strptime(end_24, "%H:%M")
            duration = (end_dt - start_dt).seconds / 3600
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


def close_popup(driver: webdriver.Chrome):
    """Close any open popup/modal."""
    try:
        close_selectors = [
            (By.CSS_SELECTOR, "button.close"),
            (By.CSS_SELECTOR, "button[aria-label='Close']"),
            (By.CSS_SELECTOR, ".popover button"),
            (By.CSS_SELECTOR, "a.close"),
            (By.XPATH, "//button[contains(text(),'×')]"),
            (By.XPATH, "//button[contains(text(),'Close')]"),
        ]
        for by, selector in close_selectors:
            try:
                btn = driver.find_element(by, selector)
                if btn.is_displayed():
                    btn.click()
                    return
            except NoSuchElementException:
                continue

        # Click elsewhere to dismiss
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass


def convert_to_24h(time_str: str) -> str:
    """Convert '7:30 AM' to '07:30'."""
    try:
        t = datetime.strptime(time_str.strip(), "%I:%M %p")
        return t.strftime("%H:%M")
    except ValueError:
        try:
            t = datetime.strptime(time_str.strip(), "%I:%M%p")
            return t.strftime("%H:%M")
        except ValueError:
            return time_str


# ── Main Collection Loop ──────────────────────────────────────────────────────

def collect_all_days(
    driver: webdriver.Chrome,
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """Collect booking data for every day in the range."""
    all_records = []
    current = start_date
    total_days = (end_date - start_date).days + 1
    day_count = 0

    log.info(f"Collecting {total_days} days: {start_date.date()} → {end_date.date()}")

    while current <= end_date:
        day_count += 1
        log.info(f"[Day {day_count}/{total_days}] {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})")

        if navigate_to_schedule(driver, current):
            records = extract_bookings(driver, current)
            all_records.extend(records)
            log.info(f"  → {len(records)} bookings found in target buildings")
        else:
            log.warning(f"  → Could not load schedule")

        current += timedelta(days=1)
        time.sleep(1)

    if not all_records:
        log.warning("No bookings collected.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates()
    df = df.sort_values(["date", "building", "room_name", "start_time"]).reset_index(drop=True)
    return df


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape Rollins EMS room bookings using Selenium browser automation."
    )
    parser.add_argument("--start", type=str, default="2026-03-07",
                        help="Start date (YYYY-MM-DD). Default: 2026-03-07")
    parser.add_argument("--end", type=str, default="2026-04-30",
                        help="End date (YYYY-MM-DD). Default: 2026-04-30")
    parser.add_argument("--output", type=str, default="reservations.csv",
                        help="Output CSV file. Default: reservations.csv")
    parser.add_argument("--username", type=str, required=True,
                        help="Rollins College username")
    parser.add_argument("--password", type=str, required=True,
                        help="Rollins College password")
    return parser.parse_args()


def main():
    args = parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    print(f"\n🔍 Rollins EMS Selenium Scraper")
    print(f"   Buildings  : {', '.join(TARGET_BUILDINGS)}")
    print(f"   Date range : {start_date.date()} → {end_date.date()}")
    print(f"   Output     : {args.output}")
    print(f"   ⚠️  A Chrome window will open. Don't touch it while scraping.\n")

    driver = create_driver()

    try:
        if not login(driver, args.username, args.password):
            log.error("Login failed. Exiting.")
            return

        df = collect_all_days(driver, start_date, end_date)

        if not df.empty:
            df.to_csv(args.output, index=False)
            print(f"\n{'='*60}")
            print(f"RESULTS")
            print(f"{'='*60}")
            print(f"Total bookings collected : {len(df)}")
            print(f"Date range               : {df['date'].min()} → {df['date'].max()}")
            print(f"Buildings                : {df['building'].nunique()}")
            print(f"Unique rooms             : {df['room_name'].nunique()}")
            print(f"\nBookings by building:")
            print(df['building'].value_counts().to_string())
            print(f"\nBookings by day of week:")
            print(df['day_of_week'].value_counts().to_string())
            print(f"\nBookings by hour:")
            print(df['hour_of_day'].value_counts().sort_index().to_string())
            print(f"\nSaved to: {args.output}")
            print(f"{'='*60}")
        else:
            print("\n⚠️  No bookings collected. See log above for details.")

    finally:
        driver.quit()
        log.info("Browser closed.")


if __name__ == "__main__":
    main()
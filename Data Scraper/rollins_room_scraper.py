"""
Rollins College Olin Library - Room Reservation Data Scraper
=============================================================
Collects reservation timestamp data from the EMS booking system
at rollins.emscloudservice.com for statistical analysis of room usage.

Usage:
    python rollins_room_scraper.py                    # scrape today
    python rollins_room_scraper.py --days 30          # scrape last 30 days
    python rollins_room_scraper.py --start 2024-01-01 --end 2024-12-31
    python rollins_room_scraper.py --output my_data.csv

HOW TO OPERATE**********************************************************************************************
# Install dependencies
pip install requests beautifulsoup4 pandas numpy

# Scrape with your Rollins login (most likely required)
python rollins_room_scraper.py --username yournetid --password yourpass --days 90 --output reservations.csv

# Or a specific date range
python rollins_room_scraper.py --username yournetid --password yourpass --start 2024-09-01 --end 2025-05-01
************************************************************************************************************

Output:
    CSV file with columns:
        date, day_of_week, start_time, end_time, duration_hours,
        room_name, room_id, hour_of_day, month, week_of_year
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
import json
import re
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL = "https://rollins.emscloudservice.com/web"

# Known rooms from the library website
KNOWN_ROOMS = {
    "Room 104": "Edwin O. Grover Classroom",
    "Room 211": "Lakeview/TWC Classroom",
    "Room 230": "Library Meeting Room",
    "Room 311": "Van Houten Conference Room",
    "Room 319": "General Classroom",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── Session & Login ────────────────────────────────────────────────────────────

def create_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def login(session: requests.Session, username: str, password: str) -> bool:
    """
    Attempt to log in to the EMS system.
    Required for accessing reservation details beyond public availability view.
    Returns True if login successful, False otherwise.
    """
    try:
        # First, get the login page to capture any CSRF tokens
        resp = session.get(f"{BASE_URL}/Default.aspx", timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract hidden form fields (ViewState, tokens, etc.)
        form_data = {}
        for inp in soup.find_all("input", type="hidden"):
            if inp.get("name") and inp.get("value"):
                form_data[inp["name"]] = inp["value"]

        # Add credentials
        form_data.update({
            "UserName": username,
            "Password": password,
        })

        # Find the login form action
        form = soup.find("form")
        action = form.get("action", "/web/Default.aspx") if form else "/web/Default.aspx"
        login_url = f"https://rollins.emscloudservice.com{action}"

        login_resp = session.post(login_url, data=form_data, timeout=15)
        login_resp.raise_for_status()

        # Check if login succeeded by looking for logout link or user name
        if "logout" in login_resp.text.lower() or username.lower() in login_resp.text.lower():
            log.info("Login successful.")
            return True
        else:
            log.warning("Login may have failed — check credentials.")
            return False

    except requests.RequestException as e:
        log.error(f"Login error: {e}")
        return False


# ── Public Availability Grid Scraper ──────────────────────────────────────────

def fetch_availability_page(session: requests.Session, date: datetime) -> str | None:
    """
    Fetch the room availability grid for a specific date.
    This page is often publicly accessible without login.
    """
    date_str = date.strftime("%m/%d/%Y")
    params = {
        "date": date_str,
        "view": "day",
    }

    # Try multiple known EMS endpoint patterns
    endpoints = [
        f"{BASE_URL}/BrowseAvailability.aspx",
        f"{BASE_URL}/RoomRequest.aspx",
        f"{BASE_URL}/Browse/BrowseAvailability",
    ]

    for endpoint in endpoints:
        try:
            resp = session.get(endpoint, params=params, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 500:
                log.info(f"  Fetched availability page from {endpoint} for {date_str}")
                return resp.text
        except requests.RequestException:
            continue

    log.warning(f"  Could not fetch availability for {date_str}")
    return None


def parse_time_slots(html: str, date: datetime) -> list[dict]:
    """
    Parse the EMS availability grid HTML to extract reservation time slots.
    Returns a list of reservation records.
    """
    records = []
    if not html:
        return records

    soup = BeautifulSoup(html, "html.parser")

    # ── Strategy 1: Look for reservation/booking cells in a grid table ────────
    # EMS grids typically use a table with time slots as rows and rooms as columns

    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        # Try to detect header row with room names
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

        if not any(room_keyword in " ".join(headers).lower()
                   for room_keyword in ["room", "104", "211", "230", "311", "319", "grover", "lakeview"]):
            continue

        # Parse time column + reservation cells
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            time_text = cells[0].get_text(strip=True)
            time_match = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", time_text, re.IGNORECASE)
            if not time_match:
                continue

            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            meridiem = time_match.group(3)
            if meridiem and meridiem.upper() == "PM" and hour != 12:
                hour += 12
            elif meridiem and meridiem.upper() == "AM" and hour == 12:
                hour = 0

            slot_time = date.replace(hour=hour, minute=minute, second=0, microsecond=0)

            # Check each room column for a booking indicator
            for col_idx, cell in enumerate(cells[1:], start=1):
                col_name = headers[col_idx] if col_idx < len(headers) else f"Room_{col_idx}"

                cell_classes = " ".join(cell.get("class", []))
                cell_text = cell.get_text(strip=True)

                # EMS typically marks reserved slots with specific CSS classes
                is_reserved = any(keyword in cell_classes.lower()
                                  for keyword in ["reserved", "booked", "unavailable", "occupied", "event"])
                is_reserved = is_reserved or any(keyword in cell_text.lower()
                                                  for keyword in ["reserved", "booked", "unavailable"])

                if is_reserved:
                    records.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "day_of_week": date.strftime("%A"),
                        "start_time": slot_time.strftime("%H:%M"),
                        "end_time": (slot_time + timedelta(minutes=30)).strftime("%H:%M"),
                        "duration_hours": 0.5,
                        "room_name": col_name,
                        "room_id": col_name,
                        "hour_of_day": hour,
                        "month": date.month,
                        "week_of_year": date.isocalendar()[1],
                        "source": "grid_table",
                    })

    # ── Strategy 2: Look for JSON data embedded in the page ──────────────────
    scripts = soup.find_all("script")
    for script in scripts:
        script_text = script.get_text()

        # Look for reservation arrays or objects in JS
        json_matches = re.findall(r'(\[{.*?}\])', script_text, re.DOTALL)
        for match in json_matches:
            try:
                data = json.loads(match)
                for item in data:
                    if isinstance(item, dict):
                        record = extract_from_json_item(item, date)
                        if record:
                            records.append(record)
            except (json.JSONDecodeError, ValueError):
                continue

    # ── Strategy 3: Look for event/reservation list items ────────────────────
    event_selectors = [
        {"class": re.compile(r"event|reservation|booking", re.I)},
        {"data-type": re.compile(r"reservation|event", re.I)},
    ]
    for selector in event_selectors:
        for element in soup.find_all(attrs=selector):
            record = extract_from_element(element, date)
            if record:
                records.append(record)

    return records


def extract_from_json_item(item: dict, date: datetime) -> dict | None:
    """Try to extract reservation data from a JSON object."""
    # Look for time-related keys
    time_keys = ["startTime", "start_time", "StartTime", "start", "Begin", "beginTime"]
    end_keys = ["endTime", "end_time", "EndTime", "end", "End", "endTime"]
    room_keys = ["roomName", "room_name", "RoomName", "room", "Room", "location"]

    start_val = next((item[k] for k in time_keys if k in item), None)
    end_val = next((item[k] for k in end_keys if k in item), None)
    room_val = next((item[k] for k in room_keys if k in item), "Unknown")

    if not start_val:
        return None

    try:
        start_dt = parse_time_string(str(start_val), date)
        end_dt = parse_time_string(str(end_val), date) if end_val else start_dt + timedelta(hours=1)
        duration = (end_dt - start_dt).seconds / 3600

        return {
            "date": date.strftime("%Y-%m-%d"),
            "day_of_week": date.strftime("%A"),
            "start_time": start_dt.strftime("%H:%M"),
            "end_time": end_dt.strftime("%H:%M"),
            "duration_hours": round(duration, 2),
            "room_name": str(room_val),
            "room_id": str(item.get("roomId", item.get("room_id", ""))),
            "hour_of_day": start_dt.hour,
            "month": date.month,
            "week_of_year": date.isocalendar()[1],
            "source": "json_embed",
        }
    except (ValueError, TypeError):
        return None


def extract_from_element(element, date: datetime) -> dict | None:
    """Extract reservation data from an HTML element."""
    text = element.get_text(separator=" ", strip=True)

    # Try to find time patterns like "9:00 AM - 10:30 AM"
    time_pattern = re.search(
        r"(\d{1,2}:\d{2}\s*(?:AM|PM)?)\s*[-–to]+\s*(\d{1,2}:\d{2}\s*(?:AM|PM)?)",
        text, re.IGNORECASE
    )
    if not time_pattern:
        return None

    try:
        start_dt = parse_time_string(time_pattern.group(1), date)
        end_dt = parse_time_string(time_pattern.group(2), date)
        duration = (end_dt - start_dt).seconds / 3600

        # Try to find room name
        room_match = re.search(r"Room\s*(\d{3})|(\b(?:Grover|Lakeview|Van Houten|TWC)\b)",
                                text, re.IGNORECASE)
        room_name = room_match.group(0) if room_match else element.get("data-room", "Unknown")

        return {
            "date": date.strftime("%Y-%m-%d"),
            "day_of_week": date.strftime("%A"),
            "start_time": start_dt.strftime("%H:%M"),
            "end_time": end_dt.strftime("%H:%M"),
            "duration_hours": round(duration, 2),
            "room_name": room_name,
            "room_id": "",
            "hour_of_day": start_dt.hour,
            "month": date.month,
            "week_of_year": date.isocalendar()[1],
            "source": "html_element",
        }
    except (ValueError, TypeError):
        return None


def parse_time_string(time_str: str, base_date: datetime) -> datetime:
    """Parse various time string formats into a datetime object."""
    time_str = time_str.strip()
    formats = [
        "%I:%M %p", "%I:%M%p", "%H:%M", "%I %p",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            t = datetime.strptime(time_str, fmt)
            return base_date.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse time string: {time_str!r}")


# ── Multi-Day Collection ───────────────────────────────────────────────────────

def collect_date_range(
    session: requests.Session,
    start_date: datetime,
    end_date: datetime,
    delay: float = 1.5,
) -> pd.DataFrame:
    """
    Collect reservation data across a date range.
    delay: seconds to wait between requests (be respectful to the server).
    """
    all_records = []
    current = start_date
    total_days = (end_date - start_date).days + 1

    log.info(f"Collecting data for {total_days} days: {start_date.date()} → {end_date.date()}")

    while current <= end_date:
        log.info(f"  Scraping {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})...")

        html = fetch_availability_page(session, current)
        if html:
            records = parse_time_slots(html, current)
            all_records.extend(records)
            log.info(f"    Found {len(records)} reservation slots.")
        else:
            log.warning(f"    No data retrieved for {current.date()}")

        current += timedelta(days=1)
        time.sleep(delay)  # Respectful delay between requests

    if not all_records:
        log.warning("No reservation data was collected. The EMS system may require login.")
        log.warning("Run with --username and --password flags if you have Rollins credentials.")
        return pd.DataFrame(columns=[
            "date", "day_of_week", "start_time", "end_time",
            "duration_hours", "room_name", "room_id",
            "hour_of_day", "month", "week_of_year", "source"
        ])

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates()
    df = df.sort_values(["date", "room_name", "start_time"]).reset_index(drop=True)
    return df


# ── Data Enrichment ────────────────────────────────────────────────────────────

def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add extra analytical columns useful for statistical analysis."""
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["is_weekend"] = df["date"].dt.dayofweek >= 5
    df["academic_semester"] = df["month"].apply(classify_semester)

    # Convert start_time string to actual hour float for finer analysis
    df["start_hour_decimal"] = df["start_time"].apply(
        lambda t: int(t.split(":")[0]) + int(t.split(":")[1]) / 60
        if isinstance(t, str) and ":" in t else None
    )

    return df


def classify_semester(month: int) -> str:
    """Classify a month into an academic semester."""
    if month in [8, 9, 10, 11, 12]:
        return "Fall"
    elif month in [1, 2, 3, 4, 5]:
        return "Spring"
    else:
        return "Summer"


# ── Output & Summary ───────────────────────────────────────────────────────────

def save_and_summarize(df: pd.DataFrame, output_path: str):
    """Save the DataFrame to CSV and print a summary."""
    df.to_csv(output_path, index=False)
    log.info(f"\nData saved to: {output_path}")

    if df.empty:
        print("\n⚠️  No reservation data collected (see notes below).")
        return

    print("\n" + "="*60)
    print("COLLECTION SUMMARY")
    print("="*60)
    print(f"Total reservation slots collected : {len(df)}")
    print(f"Date range                         : {df['date'].min()} → {df['date'].max()}")
    print(f"Unique rooms found                 : {df['room_name'].nunique()}")
    print(f"Rooms: {', '.join(df['room_name'].unique())}")
    print(f"\nReservations by day of week:")
    print(df['day_of_week'].value_counts().to_string())
    print(f"\nReservations by hour of day:")
    print(df['hour_of_day'].value_counts().sort_index().to_string())
    print("="*60)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape room reservation timestamps from Rollins Olin Library EMS system."
    )
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD). Default: today.")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD). Default: today.")
    parser.add_argument("--days", type=int, default=1,
                        help="Number of past days to collect (alternative to --start/--end).")
    parser.add_argument("--output", type=str, default="reservations.csv",
                        help="Output CSV file path. Default: reservations.csv")
    parser.add_argument("--username", type=str, default=None,
                        help="Rollins College username (for authenticated access).")
    parser.add_argument("--password", type=str, default=None,
                        help="Rollins College password (for authenticated access).")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Delay in seconds between requests. Default: 1.5")
    return parser.parse_args()


def main():
    args = parse_args()

    # Determine date range
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    elif args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = today
    else:
        end_date = today
        start_date = today - timedelta(days=args.days - 1)

    print(f"\n🔍 Rollins Library Room Reservation Scraper")
    print(f"   Date range : {start_date.date()} → {end_date.date()}")
    print(f"   Output     : {args.output}\n")

    session = create_session()

    # Attempt login if credentials provided
    if args.username and args.password:
        login(session, args.username, args.password)
    else:
        log.info("Running without login (public data only). Use --username/--password for more data.")

    # Collect data
    df = collect_date_range(session, start_date, end_date, delay=args.delay)

    # Enrich with analytical columns
    df = enrich_dataframe(df)

    # Save results
    save_and_summarize(df, args.output)

    if df.empty:
        print("""
NOTES ON DATA COLLECTION:
─────────────────────────────────────────────────────────────
The Rollins EMS reservation system (rollins.emscloudservice.com)
may require a valid Rollins College login to view reservations.

Options to collect data:
  1. Run with credentials:
       python rollins_room_scraper.py --username YOURNETID --password YOURPASS --days 30

  2. If you are a Rollins student, you can log in to EMS manually,
     then export reservation data directly from the system interface.

  3. Contact the Olin Library (olinOFC@rollins.edu) and explain
     your CS research project — they may share anonymized data.

  4. For your CS project, you can also generate synthetic data
     based on published library hours as a baseline dataset.
─────────────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()

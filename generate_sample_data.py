"""
Rollins Library — Synthetic Data Generator + Analysis Starter
==============================================================
Generates a realistic synthetic reservation dataset based on
Rollins College library hours and typical academic usage patterns.

Use this to:
  - Test your statistical analysis pipeline while waiting for real data
  - Supplement real scraped data
  - Validate your analysis logic

Run:
    python generate_sample_data.py
    python generate_sample_data.py --year 2024 --output sample_reservations.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import random

random.seed(42)
np.random.seed(42)

# ── Library configuration ──────────────────────────────────────────────────────

ROOMS = [
    "Room 104 - Edwin O. Grover Classroom",
    "Room 211 - Lakeview/TWC Classroom",
    "Room 230 - Library Meeting Room",
    "Room 311 - Van Houten Conference Room",
    "Room 319 - General Classroom",
]

# Library hours: (open_hour, close_hour) by day of week (0=Mon, 6=Sun)
LIBRARY_HOURS = {
    0: (8, 22),   # Monday
    1: (8, 22),   # Tuesday
    2: (8, 22),   # Wednesday
    3: (8, 22),   # Thursday
    4: (8, 20),   # Friday
    5: (10, 18),  # Saturday
    6: (12, 20),  # Sunday
}

# Academic calendar (month: relative busyness multiplier)
SEMESTER_BUSYNESS = {
    1: 0.6,   # January — exam period / start of spring
    2: 0.85,
    3: 0.90,
    4: 0.95,  # April — finals approaching
    5: 0.75,  # May — finals + end of spring
    6: 0.30,  # Summer
    7: 0.30,
    8: 0.55,  # August — start of fall
    9: 0.80,
    10: 0.90,
    11: 0.95, # November — fall finals approaching
    12: 0.50, # December — finals + break
}

# Hour of day demand curve (normalized 0–1)
# Peaks at midday and early evening
HOUR_DEMAND = {
    8:  0.10,
    9:  0.30,
    10: 0.55,
    11: 0.70,
    12: 0.80,
    13: 0.85,
    14: 0.90,
    15: 0.95,
    16: 0.90,
    17: 0.80,
    18: 0.65,
    19: 0.50,
    20: 0.30,
    21: 0.15,
}

# Typical booking durations in hours (weighted)
DURATION_OPTIONS = [0.5, 1.0, 1.0, 1.5, 1.5, 2.0, 2.0, 2.5, 3.0]


def classify_semester(month: int) -> str:
    if month in [8, 9, 10, 11, 12]:
        return "Fall"
    elif month in [1, 2, 3, 4, 5]:
        return "Spring"
    else:
        return "Summer"


def is_holiday(date: datetime) -> bool:
    """Check if a date is a major US holiday when library might be closed."""
    holidays = [
        (1, 1),   # New Year's Day
        (7, 4),   # Independence Day
        (11, 25), # Approximate Thanksgiving
        (12, 25), # Christmas
        (12, 26),
        (12, 27),
        (12, 28),
        (12, 29),
        (12, 30),
        (12, 31),
    ]
    return (date.month, date.day) in holidays


def generate_day_reservations(date: datetime) -> list[dict]:
    """Generate synthetic reservations for a single day."""
    records = []
    dow = date.weekday()

    if is_holiday(date):
        return records

    open_hour, close_hour = LIBRARY_HOURS[dow]
    month_factor = SEMESTER_BUSYNESS.get(date.month, 0.5)
    is_weekend = dow >= 5

    for room in ROOMS:
        # Track which half-hour slots are already booked
        booked_slots = set()
        total_slots = (close_hour - open_hour) * 2  # 30-min slots

        # Determine how many bookings this room gets today
        base_bookings = 6 if not is_weekend else 3
        n_bookings = max(0, int(np.random.poisson(base_bookings * month_factor)))

        for _ in range(n_bookings):
            # Pick a start hour weighted by demand curve
            available_hours = [h for h in range(open_hour, close_hour - 1)]
            demand_weights = [HOUR_DEMAND.get(h, 0.1) for h in available_hours]

            if not available_hours:
                break

            start_hour = random.choices(available_hours, weights=demand_weights, k=1)[0]
            start_minute = random.choice([0, 30])
            duration = random.choice(DURATION_OPTIONS)

            start_slot = (start_hour - open_hour) * 2 + (1 if start_minute == 30 else 0)
            n_slots = int(duration * 2)

            # Check if slots are free
            slots_needed = set(range(start_slot, start_slot + n_slots))
            if slots_needed & booked_slots:
                continue  # Conflict, skip
            if start_slot + n_slots > total_slots:
                continue  # Beyond closing

            booked_slots |= slots_needed

            start_dt = date.replace(hour=start_hour, minute=start_minute)
            end_dt = start_dt + timedelta(hours=duration)

            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "day_of_week": date.strftime("%A"),
                "start_time": start_dt.strftime("%H:%M"),
                "end_time": end_dt.strftime("%H:%M"),
                "duration_hours": duration,
                "room_name": room,
                "room_id": room.split(" - ")[0],
                "hour_of_day": start_hour,
                "month": date.month,
                "week_of_year": date.isocalendar()[1],
                "is_weekend": is_weekend,
                "academic_semester": classify_semester(date.month),
                "source": "synthetic",
            })

    return records


def generate_year(year: int) -> pd.DataFrame:
    """Generate synthetic reservation data for an entire year."""
    all_records = []
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    current = start
    total = (end - start).days + 1

    print(f"Generating synthetic data for {year} ({total} days)...")

    while current <= end:
        records = generate_day_reservations(current)
        all_records.extend(records)
        current += timedelta(days=1)

    df = pd.DataFrame(all_records)
    df = df.sort_values(["date", "room_name", "start_time"]).reset_index(drop=True)
    return df


def print_summary(df: pd.DataFrame):
    print("\n" + "="*60)
    print("SYNTHETIC DATASET SUMMARY")
    print("="*60)
    print(f"Total reservations    : {len(df):,}")
    print(f"Date range            : {df['date'].min()} → {df['date'].max()}")
    print(f"Unique rooms          : {df['room_name'].nunique()}")
    print(f"\nBy Semester:")
    print(df.groupby("academic_semester").size().to_string())
    print(f"\nBy Day of Week:")
    day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    day_counts = df['day_of_week'].value_counts().reindex(day_order)
    print(day_counts.to_string())
    print(f"\nBy Hour of Day (top 5 busiest):")
    print(df['hour_of_day'].value_counts().head(5).sort_index().to_string())
    print(f"\nAverage duration (hours): {df['duration_hours'].mean():.2f}")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic library reservation data.")
    parser.add_argument("--year", type=int, default=datetime.today().year - 1,
                        help="Year to generate data for.")
    parser.add_argument("--output", type=str, default="sample_reservations.csv",
                        help="Output CSV file path.")
    args = parser.parse_args()

    df = generate_year(args.year)
    df.to_csv(args.output, index=False)
    print(f"\nSaved to: {args.output}")
    print_summary(df)


if __name__ == "__main__":
    main()

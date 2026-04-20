"""
Bridge between the ML model and Django views.
"""

import os
from functools import lru_cache


# ML_DIR 

ML_DIR = os.path.join(os.path.dirname(__file__), "ml")


@lru_cache(maxsize=1)
def _load_model_and_encoders():
    """
    Load the trained Random Forest model and label encoders from disk.
    Uses @lru_cache so the heavy .joblib files are read only once.
    """
    from predictor.ml.room_predictor import load_model

    import predictor.ml.room_predictor as rp
    rp.MODEL_PATH = os.path.join(ML_DIR, "room_model.joblib")
    rp.ENCODER_PATH = os.path.join(ML_DIR, "label_encoders.joblib")

    model, encoders = load_model()
    return model, encoders


# Maps day names to numeric codes (same as room_predictor.py)
DAY_NAMES = {
    "monday": 0, "tuesday": 1, "wednesday": 2,
    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}
DAY_CHOICES = [(str(v), k.capitalize()) for k, v in DAY_NAMES.items()]

MONTH_CHOICES = [
    ("1", "January"), ("2", "February"), ("3", "March"),
    ("4", "April"), ("5", "May"), ("6", "June"),
    ("7", "July"), ("8", "August"), ("9", "September"),
    ("10", "October"), ("11", "November"), ("12", "December"),
]


def get_forecast(building_full_name: str, room_full_name: str,
                 day_of_week: int, month: int,
                 start_hour: int = 7, end_hour: int = 22) -> list[dict]:
    """
    Return a list of hourly predictions for a room on a given day/month.

    Raises ValueError if building or room is missing/empty (the model has no
    sensible default and we must not silently pick one). Unknown building/room
    names also raise ValueError via the underlying encoder check.
    """
    from predictor.ml.room_predictor import predict_slot

    # Hard contract: BOTH building and room are required. No defaults.
    if not building_full_name or not str(building_full_name).strip():
        raise ValueError("building is required for prediction")
    if not room_full_name or not str(room_full_name).strip():
        raise ValueError("room is required for prediction")

    model, encoders = _load_model_and_encoders()
    results = []

    for hour in range(start_hour, end_hour + 1):
        pred = predict_slot(model, encoders,
                            building_full_name, room_full_name,
                            day_of_week, month, hour)

        if hour == 0:
            display = "12:00 AM"
        elif hour < 12:
            display = f"{hour}:00 AM"
        elif hour == 12:
            display = "12:00 PM"
        else:
            display = f"{hour - 12}:00 PM"

        results.append({
            "hour": hour,
            "hour_display": display,
            "reserved": pred["reserved"],
            "probability": pred["probability"],
            "pct": int(pred["probability"] * 100),
        })

    return results


def get_known_buildings() -> list[str]:
    """Return the list of building names the model was trained on."""
    _, encoders = _load_model_and_encoders()
    return list(encoders["building"].classes_)


def get_known_rooms() -> list[str]:
    """Return the list of room names the model was trained on."""
    _, encoders = _load_model_and_encoders()
    return list(encoders["room"].classes_)


def get_building_room_mapping() -> dict[str, list[str]]:
    """Return a dict mapping each building to its list of rooms."""
    import pandas as pd
    data_path = os.path.join(ML_DIR, "cleaned_data.csv")
    df = pd.read_csv(data_path)
    mapping = {}
    for building, group in df.groupby("building"):
        mapping[building] = sorted(group["room"].unique().tolist())
    return mapping
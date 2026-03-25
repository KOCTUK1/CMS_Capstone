"""
room_predictor.py
Trains on cleaned EMS booking data to predict whether a given room will be reserved at a specific day/time.
Usage:
    # Train and save the model; run once and it will save
    python room_predictor.py --train
 
    # Predict a single slot by the hour
    python room_predictor.py --predict \
        --building "Bush Science Center" \
        --room "Room 212" \
        --day Monday \
        --month 10 \
        --hour 14
 
    # Show a full day prediction for a room
    python room_predictor.py --forecast \
        --building "Olin Library" \
        --room "Room 230 - Library Meeting Room" \
        --day Wednesday \
        --month 3
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import os
import argparse
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder
 
# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "cleaned_data.csv")
MODEL_PATH = os.path.join(BASE_DIR, "room_model.joblib")
ENCODER_PATH = os.path.join(BASE_DIR, "label_encoders.joblib")
 
# ---------------------------------------------------------------------------
# Encodings for strings to numbers 
# ---------------------------------------------------------------------------
DAY_NAMES = {
    "monday": 0, "tuesday": 1, "wednesday": 2,
    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}
DAY_LABELS = {v: k.capitalize() for k, v in DAY_NAMES.items()}
 
FEATURES = ["day_of_week", "month", "hour", "building_enc", "room_enc"]
 
# ---------------------------------------------------------------------------
# Data loading & encoding
# ---------------------------------------------------------------------------
def load_and_encode(data_path: str):
    """Load cleaned_data.csv and label-encode building/room columns."""
    df = pd.read_csv(data_path)
 
    building_enc = LabelEncoder()
    room_enc = LabelEncoder()
 
    df["building_enc"] = building_enc.fit_transform(df["building"])
    df["room_enc"] = room_enc.fit_transform(df["room"])
 
    encoders = {"building": building_enc, "room": room_enc}
    return df, encoders
    
# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train(data_path: str = DATA_PATH):
    """Train the Random Forest and persist the model + encoders to disk."""
    print("Loading data ...")
    df, encoders = load_and_encode(data_path)
 
    X = df[FEATURES]
    y = df["reserved"]
 
    print(f"  Total slots : {len(df):,}")
    print(f"  Reserved    : {y.sum():,}  ({y.mean()*100:.1f}%)")
    print(f"  Free        : {(~y.astype(bool)).sum():,}\n")
 
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
 
    print("Training Random Forest ...")
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        class_weight="balanced",  # counteracts the imbalance in open/reserved frequencies
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    print("  Done.\n")
 
    # Basic report on training
    y_pred = model.predict(X_test)
    print("=== Classification Report ===")
    print(classification_report(y_test, y_pred, target_names=["Free", "Reserved"]))
 
    # Persist model and encoders
    joblib.dump(model, MODEL_PATH)
    joblib.dump(encoders, ENCODER_PATH)
    print(f"Model saved → {MODEL_PATH}")
    print(f"Encoders saved → {ENCODER_PATH}")
    print("\nRun evaluate_model.py to generate plots and detailed metrics.")
    
# ---------------------------------------------------------------------------
# Inference helpers
# ---------------------------------------------------------------------------
def load_model():
    """Load the persisted model and encoders. Raises if not trained yet."""
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            "No trained model found. Run: python room_predictor.py --train"
        )
    model = joblib.load(MODEL_PATH)
    encoders = joblib.load(ENCODER_PATH)
    return model, encoders
 
def encode_input(building: str, room: str, encoders: dict):
    """Encode a building/room name. Returns (building_enc, room_enc) or raises."""
    known_buildings = list(encoders["building"].classes_)
    known_rooms = list(encoders["room"].classes_)
 
    if building not in known_buildings:
        raise ValueError(
            f"Unknown building: '{building}'\n"
            f"Known buildings: {known_buildings}"
        )
    if room not in known_rooms:
        raise ValueError(
            f"Unknown room: '{room}'\n"
            f"Known rooms for lookup:\n" + "\n".join(known_rooms)
        )
 
    b_enc = encoders["building"].transform([building])[0]
    r_enc = encoders["room"].transform([room])[0]
    return b_enc, r_enc
    
def predict_slot(model, encoders, building: str, room: str,
                 day_of_week: int, month: int, hour: int) -> dict:
    """Return prediction + probability for a single time slot."""
    b_enc, r_enc = encode_input(building, room, encoders)
 
    X = pd.DataFrame([{
        "day_of_week": day_of_week,
        "month": month,
        "hour": hour,
        "building_enc": b_enc,
        "room_enc": r_enc,
    }])
 
    prob = model.predict_proba(X)[0][1]
    predicted = int(prob >= 0.5)
 
    return {
        "building": building,
        "room": room,
        "day": DAY_LABELS[day_of_week],
        "month": month,
        "hour": hour,
        "reserved": bool(predicted),
        "probability": round(prob, 3),
    }

def forecast_day(model, encoders, building: str, room: str,
                 day_of_week: int, month: int,
                 start_hour: int = 7, end_hour: int = 22) -> pd.DataFrame:
    """Return a full-day forecast for a room as a DataFrame."""
    rows = []
    for hour in range(start_hour, end_hour + 1):
        result = predict_slot(model, encoders, building, room,
                              day_of_week, month, hour)
        rows.append(result)
    return pd.DataFrame(rows)
    
def get_known_buildings(encoders: dict) -> list:
    """Return the list of building names the model knows about."""
    return list(encoders["building"].classes_)
    
def get_known_rooms(encoders: dict) -> list:
    """Return the list of room names the model knows about."""
    return list(encoders["room"].classes_)
    
# ---------------------------------------------------------------------------
# CLI display helper functions
# ---------------------------------------------------------------------------
def print_slot(result: dict):
    status = "RESERVED" if result["reserved"] else "FREE"
    print(
        f"\n  {result['building']} — {result['room']}\n"
        f"  {result['day']}, Month {result['month']}, "
        f"{result['hour']:02d}:00\n"
        f"  Status      : {status}\n"
        f"  Probability : {result['probability']*100:.1f}% chance reserved\n"
    )
    
def print_forecast(df: pd.DataFrame):
    building = df.iloc[0]["building"]
    room = df.iloc[0]["room"]
    day = df.iloc[0]["day"]
    month = df.iloc[0]["month"]
 
    print(f"\n Forecast — {building}\n {room}")
    print(f" {day}, Month {month}\n")
    print(f" {'Hour':<8} {'Status':<12} {'Probability':>12}")
    print("  " + "-" * 36)
    for _, row in df.iterrows():
        status = "Reserved" if row["reserved"] else "Free"
        bar_len = int(row["probability"] * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(
            f" {row['hour']:02d}:00 {status:<12} "
            f"{row['probability']*100:>5.1f}% {bar}"
        )
    print()
    
# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Room Reservation Predictor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--train", action="store_true", help="Train and save the model")
    mode.add_argument("--predict", action="store_true", help="Predict a single time slot")
    mode.add_argument("--forecast", action="store_true", help="Forecast all hours for a room/day")
 
    parser.add_argument("--building", type=str, help="Building name")
    parser.add_argument("--room", type=str, help="Room name")
    parser.add_argument("--day", type=str, help="Day of week (e.g. Monday)")
    parser.add_argument("--month", type=int, help="Month number (1-12)")
    parser.add_argument("--hour", type=int, help="Hour in 24h format (--predict only)")
    parser.add_argument("--data", type=str, default=DATA_PATH, help="Path to cleaned_data.csv (default: same directory)")
    return parser.parse_args()
    
def main():
    args = parse_args()
 
    if args.train:
        train(args.data)
        return
 
    # validation
    for flag in ("building", "room", "day", "month"):
        if not getattr(args, flag):
            raise SystemExit(f"--{flag} is required for --predict / --forecast")
 
    day_key = args.day.lower()
    if day_key not in DAY_NAMES:
        raise SystemExit(
            f"Unknown day '{args.day}'. "
            f"Use one of: {', '.join(d.capitalize() for d in DAY_NAMES)}"
        )
    day_of_week = DAY_NAMES[day_key]
 
    model, encoders = load_model()
 
    if args.predict:
        if args.hour is None:
            raise SystemExit("--hour is required for --predict")
        result = predict_slot(
            model, encoders,
            args.building, args.room,
            day_of_week, args.month, args.hour,
        )
        print_slot(result)
 
    elif args.forecast:
        df = forecast_day(
            model, encoders,
            args.building, args.room,
            day_of_week, args.month,
        )
        print_forecast(df)
        
if __name__ == "__main__":
    main()

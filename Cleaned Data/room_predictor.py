import pandas as pd
from sklearn.ensemble import RandomForestClassifier

df = pd.read_csv('rooms.csv')
print(df.head())
print(df.shape)

df['date_clean'] = df['Date'].str.split(' ').str[0]
df['date_clean'] = pd.to_datetime(df['date_clean'])

print(df['date_clean'].head())
print(df['date_clean'].dtype)

df['day_of_week'] = df['date_clean'].dt.dayofweek
df['month'] = df['date_clean'].dt.month

print(df[['Date', 'date_clean', 'day_of_week', 'month']].head())

df['start_hour'] = pd.to_datetime(df['Reserved Start']).dt.hour

print(df[['Reserved Start', 'start_hour']].head(10))

df['end_hour'] = pd.to_datetime(df['Reserved End']).dt.hour

print(df[['Reserved Start', 'start_hour', 'Reserved End', 'end_hour']].head(10))

# DATA CLEANING
hours = list(range(7, 23))

# TRAINING DATA
rows = []
for _, reservation in df.iterrows():
    for hour in range(reservation['start_hour'], reservation['end_hour']):
        rows.append({
            'day_of_week': reservation['day_of_week'],
            'month': reservation['month'],
            'hour': hour,
            'building': reservation['Building'],
            'room': reservation['Room'],
            'reserved': 1
        })

reserved_df = pd.DataFrame(rows)
print(reserved_df.head(10))
print(f"\nTotal reserved slots: {len(reserved_df)}")

everydate = df[['date_clean', 'day_of_week', 'month']].drop_duplicates()
everyroom = df[['Building', 'Room']].drop_duplicates()

zero_rows = []
for _, date_row in everydate.iterrows():
    for _, room_row in everyroom.iterrows():
        for hour in range(7, 23):
            zero_rows.append({
                'day_of_week': date_row['day_of_week'],
                'month': date_row['month'],
                'hour': hour,
                'building': room_row['Building'],
                'room': room_row['Room'],
                'reserved': 0
            })

everyslots_df = pd.DataFrame(zero_rows)
print(f"Total possible slots: {len(everyslots_df)}")

combine = pd.concat([reserved_df, everyslots_df])

combine = combine.sort_values('reserved', ascending=False).drop_duplicates(subset=['day_of_week', 'month', 'hour', 'building', 'room'])

print(f"Combined dataset: {len(combine)} rows")
print(f"Reserved: {combine['reserved'].sum()}")
print(f"Not reserved: {len(combine) - combine['reserved'].sum()}")

building_codes = {
    'Bush Science Center': 100,
    'Kathleen W Rollins Hall': 200,
    'Olin Library': 300,
}

room_codes = {
    # Bush Science Center
    'Bush Atrium - Student Priority': 2,
    'Bush Auditorium and Lobby': 1,
    'Bush Lawn (Southside of building)': 3,
    'Bush Rooftop': 4,
    'Room 102 - STEM Hub ONLY': 102,
    'Room 123 - Seminar/Green Room - Sci Div Priority': 123,
    'Room 164 - Physics Priority': 164,
    'Room 176 - Smart Classroom ': 176,
    'Room 200': 200,
    'Room 201': 201,
    'Room 202': 202,
    'Room 208': 208,
    'Room 210 ': 210,
    'Room 212': 212,
    'Room 228': 228,
    'Room 260 - Chemistry Lab Pre-Lab/Seminar Room': 260,
    'Room 274 - Operant Lab/Seminar - Operant Lab Prior': 274,
    'Room 277 - Seminar Room ': 277,
    'Room 301 - Computer Classroom (PC)': 301,
    'Room 302 ': 302,
    'Room 308 ': 308,
    'Room 310 - Computer Lab (Mac)': 310,
    # Kathleen W Rollins Hall
    '3rd Floor Lobby - Prefunction Area': 1,
    'Fountain Entrance Lobby & Gathering Stairs': 2,
    'Fox Den': 3,
    'Mills Foyer': 4,
    'Mills Lawn': 5,
    'Room 110 - Conference Room - Campus Media Area': 110,
    'Room 112 - Podcast Room - Campus Media Priority': 112,
    'Room 114 - Meeting Room - Campus Media Area': 114,
    'Room 128 - Screening Room': 128,
    'Room 131 - Conference Room - CCLP Area': 131,
    'Room 136 - Meeting Room - CCLP Area': 136,
    'Room 141 - Meeting Room - CCLP Area': 141,
    'Room 143 - Meeting Room - CCLP Area': 143,
    'Room 149 - Conference Room - Academic Adv Area': 149,
    'Room 226 - Meeting Room - CLCE/Global Init. Area': 226,
    'Room 230 - Conference Room - CLCE Area': 230,
    'Room 240/240B - Social Impact Hub/Classroom': 240,
    'Room 241 - Conference Room - Hub/SE/Acad Adv Area': 241,
    'Room 300 - Galloway Room ': 300,
    'Room 301 - Ourisman Meeting Room': 301,
    'Room 310 - Genius Scale-up Classroom': 310,
    'Room 320 - Hauske Scale-up Classroom': 320,
    'Room 330 - Teaching Seminar Room': 330,
    'Room 340 - Galloway Seminar Meeting Room': 340,
    'Tars Plaza': 6,
    # Olin Library
    'Olin Lawn': 1,
    'Room 104 - Edwin O. Grover Room': 104,
    'Room 211 - Tutoring & Writing Center': 211,
    'Room 220 - Center for Creativity (Mac) ': 220,
    'Room 225 - Teaching Computer Lab (PC)': 225,
    'Room 226 - Lobby & Lounge Area': 226,
    'Room 230 - Library Meeting Room': 230,
    'Room 319 ': 319,
    'Van Houten Conference Room': 2,
}

combine['building_code'] = combine['building'].map(building_codes)
combine['room_code'] = combine['room'].map(room_codes)

print(combine[['building', 'building_code', 'room', 'room_code']].head(10))

X = combine[['day_of_week', 'month', 'hour', 'building_code', 'room_code']]
y = combine['reserved']

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

print("Model trained!")
print(f"Features: {list(X.columns)}")

combine.to_csv('cleaned_data.csv', index=False)
print("Saved cleaned_data.csv")
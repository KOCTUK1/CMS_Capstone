import numpy as np
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import make_pipeline

data = pd.DataFrame({
    'hour': [6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],
    'crowd_level': [1,2,4,6,7,8,9,10,9,7,5,3,2,1,1,1,1]
})

X = data[['hour']]
y = data['crowd_level']

model = make_pipeline(PolynomialFeatures(degree=3), LinearRegression())
model.fit(X, y)

new_hours = pd.DataFrame({'hour': [10, 14, 19.5]})
predictions = model.predict(new_hours)

for hour, pred in zip([10,14,19.5], predictions):
    print(f"Predicted crowd level at {hour}:00 is {round(pred)}")
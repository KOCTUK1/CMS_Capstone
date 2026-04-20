# URL scheme for the predictor app

from django.urls import path
from . import views

app_name = "predictor"
urlpatterns = [
    # Page 1: building selection
    path("", views.select_building, name="select_building"),
    # Page 2: room + day + month selection + forecast result
    path("forecast/<str:building>/", views.forecast, name="forecast"),
]
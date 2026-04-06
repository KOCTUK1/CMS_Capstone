# This file defines the URL scheme

from django.urls import path

from . import views

app_name = "predictor"
urlpatterns = [
    path("", views.index, name="index"),
    path("<str:short_building_name>/", views.building_detail, name="building_detail"),
    path("<str:short_building_name>/<str:short_room_name>/", views.room_detail, name="room_detail")
]
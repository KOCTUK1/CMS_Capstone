"""
predictor/views.py
"""

from django.shortcuts import render

from .forms import ForecastForm
from .services import get_forecast


def index(request):
    forecast = None
    selected = {}
    error_message = None

    if request.GET:
        form = ForecastForm(request.GET)
        if form.is_valid():
            building = form.cleaned_data["building"]
            room = form.cleaned_data["room"]
            day_of_week = int(form.cleaned_data["day"])
            month = int(form.cleaned_data["month"])

            selected = {
                "building": building,
                "room": room,
                "day": dict(form.fields["day"].choices)[form.cleaned_data["day"]],
                "month": dict(form.fields["month"].choices)[form.cleaned_data["month"]],
            }

            try:
                forecast = get_forecast(
                    building_full_name=building,
                    room_full_name=room,
                    day_of_week=day_of_week,
                    month=month,
                )
            except ValueError as e:
                error_message = str(e)
    else:
        form = ForecastForm()

    return render(request, "predictor/index.html", {
        "form": form,
        "forecast": forecast,
        "selected": selected,
        "error_message": error_message,
    })
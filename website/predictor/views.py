"""
predictor/views.py

Two-page flow:
  Page 1 (select_building) : user picks one of the buildings the model knows.
  Page 2 (forecast)        : user picks a room + day + month for that building;
                             on submit we run the predictor and render the
                             hourly forecast on the same page.

Server-side validation guards the predictor's hard contract: both `building`
AND `room` must be present and known to the model, or we redirect back to
Page 1 / Page 2 with a user-facing error message (we never call the predictor
with partial input).
"""

from django.contrib import messages
from django.shortcuts import redirect, render

from .forms import ForecastForm, list_buildings
from .services import (
    get_building_room_mapping,
    get_forecast,
    get_known_buildings,
)


# ---------------------------------------------------------------------------
# Page 1 — building selection
# ---------------------------------------------------------------------------
def select_building(request):
    """Show the list of buildings the user can predict for."""
    buildings = list_buildings()
    return render(request, "predictor/select_building.html", {
        "buildings": buildings,
    })


# ---------------------------------------------------------------------------
# Page 2 — room + day + month selection, and forecast result
# ---------------------------------------------------------------------------
def forecast(request, building: str):
    """
    Page 2. The building comes from the URL; room/day/month come from the
    GET query string (same single-URL pattern as before — bookmarkable).

    If the URL's building is unknown we redirect to Page 1 with a message.
    If the form is missing a room, the form flags the error inline.
    """
    # ---- Validate the URL's building -------------------------------------
    known_buildings = get_known_buildings()
    if not building or building not in known_buildings:
        messages.error(
            request,
            f"Unknown building '{building}'. Please pick one from the list below.",
        )
        return redirect("predictor:select_building")

    # ---- Prepare form ----------------------------------------------------
    submitted = any(k in request.GET for k in ("room", "day", "month"))

    forecast_rows = None
    selected = {}
    error_message = None

    if submitted:
        form = ForecastForm(request.GET, building=building)
        if form.is_valid():
            room = form.cleaned_data["room"]
            day_of_week = int(form.cleaned_data["day"])
            month = int(form.cleaned_data["month"])

            selected = {
                "building": building,
                "room": room,
                "day": dict(form.fields["day"].choices)[form.cleaned_data["day"]],
                "month": dict(form.fields["month"].choices)[form.cleaned_data["month"]],
            }

            # ---- Hard contract enforcement -------------------------------
            if not building or not room:
                messages.error(
                    request,
                    "Both a building and a room are required to run a prediction.",
                )
                return redirect("predictor:forecast", building=building)

            try:
                forecast_rows = get_forecast(
                    building_full_name=building,
                    room_full_name=room,
                    day_of_week=day_of_week,
                    month=month,
                )
            except ValueError as exc:
                error_message = str(exc)
    else:
        form = ForecastForm(building=building)

    rooms = get_building_room_mapping().get(building, [])

    return render(request, "predictor/forecast.html", {
        "building": building,
        "rooms": rooms,
        "form": form,
        "forecast": forecast_rows,
        "selected": selected,
        "error_message": error_message,
    })
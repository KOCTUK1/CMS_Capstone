"""
Defines the different pages:
  - index:           list all buildings
  - building_detail: list rooms in a building
  - room_detail:     show forecast form and prediction results for a room
"""

from django.http import Http404
from django.shortcuts import get_object_or_404, render

from .models import Building, Room
from .forms import ForecastForm
from .services import get_forecast



# Page 1: List of buildings
def index(request):
    building_list = Building.objects.all()
    return render(request, "predictor/index.html", {"building_list": building_list})



# Page 2: Rooms inside a building

def building_detail(request, short_building_name):
    building = get_object_or_404(Building, short_building_name=short_building_name)
    room_list = building.room_set.all()
    return render(request, "predictor/building_detail.html",
                  {"building": building, "room_list": room_list})



# Page 3: Room detail - prediction form + forecast results

def room_detail(request, short_building_name, short_room_name):
    building = get_object_or_404(Building, short_building_name=short_building_name)
    try:
        room = building.room_set.get(short_room_name=short_room_name)
    except Room.DoesNotExist:
        raise Http404("Room does not exist")

    forecast = None      
    selected_day = None   
    selected_month = None 
    error_message = None  

    # When the user submits the form
    if "day" in request.GET and "month" in request.GET:
        form = ForecastForm(request.GET)
        if form.is_valid():
            day_of_week = int(form.cleaned_data["day"])
            month = int(form.cleaned_data["month"])

            
            selected_day = dict(form.fields["day"].choices)[form.cleaned_data["day"]]
            selected_month = dict(form.fields["month"].choices)[form.cleaned_data["month"]]

            try:
                forecast = get_forecast(
                    building_full_name=building.full_building_name,
                    room_full_name=room.full_room_name,
                    day_of_week=day_of_week,
                    month=month,
                )
            except ValueError as e:
                error_message = str(e)
    else:
        form = ForecastForm()

    return render(request, "predictor/room_detail.html", {
        "building": building,
        "room": room,
        "form": form,
        "forecast": forecast,
        "selected_day": selected_day,
        "selected_month": selected_month,
        "error_message": error_message,
    })
from django.http import Http404
from django.shortcuts import get_object_or_404, render

from .models import Building, Room

# Defines the different pages
def index(request):
    building_list = Building.objects.all()
    return render(request, "predictor/index.html", {"building_list": building_list})

def building_detail(request, short_building_name):
    building = get_object_or_404(Building, short_building_name=short_building_name)
    room_list = building.room_set.all()
    return render(request, "predictor/building_detail.html",
                  {"building": building, "room_list": room_list})

def room_detail(request, short_building_name, short_room_name):
    building = get_object_or_404(Building, short_building_name=short_building_name)
    try:
        room = building.room_set.get(short_room_name=short_room_name)
    except Room.DoesNotExist:
        raise Http404("Room does not exist")
    return render(request, "predictor/room_detail.html", {"room": room})
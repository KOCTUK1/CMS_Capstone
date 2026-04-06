from django.db import models

# Create your models here.
class Building(models.Model):
    full_building_name = models.CharField(max_length=200)
    short_building_name = models.CharField(max_length=10)
    def __str__(self):
        return self.full_building_name

class Room(models.Model):
    building = models.ForeignKey(Building, on_delete=models.CASCADE)
    full_room_name = models.CharField(max_length=200)
    short_room_name = models.CharField(max_length=10)
    def __str__(self):
        return self.full_room_name
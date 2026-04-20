"""
predictor/forms.py
"""

from django import forms
from .services import (
    DAY_CHOICES,
    MONTH_CHOICES,
    get_building_room_mapping,
    get_known_buildings,
)


class ForecastForm(forms.Form):
    """
    Page 2 form. The building is fixed by the URL, so only room/day/month are
    user-chosen here. Room choices are filtered to the rooms that belong to the
    given building according to the ML training data.
    """

    room = forms.ChoiceField(
        label="Room",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    day = forms.ChoiceField(
        choices=DAY_CHOICES,
        label="Day of week",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    month = forms.ChoiceField(
        choices=MONTH_CHOICES,
        label="Month",
        widget=forms.Select(attrs={"class": "form-select"}),
    )

    def __init__(self, *args, building: str = "", **kwargs):
        super().__init__(*args, **kwargs)
        self.building = building

        mapping = get_building_room_mapping()
        rooms = mapping.get(building, [])
        self.fields["room"].choices = [(r, r) for r in rooms]

    def clean_room(self):
        room = self.cleaned_data.get("room", "").strip()
        if not room:
            raise forms.ValidationError("Please select a room.")
        return room


def list_buildings() -> list[str]:
    """Convenience wrapper used by the Page 1 view."""
    return sorted(get_known_buildings())
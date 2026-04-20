"""
predictor/forms.py
"""

from django import forms
from .services import DAY_CHOICES, MONTH_CHOICES, get_known_buildings, get_known_rooms


class ForecastForm(forms.Form):
    building = forms.ChoiceField(
        label="Building",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Pull buildings/rooms from the ML model itself so the dropdowns
        # only show values the model can actually predict for.
        buildings = sorted(get_known_buildings())
        rooms = sorted(get_known_rooms())
        self.fields["building"].choices = [(b, b) for b in buildings]
        self.fields["room"].choices = [(r, r) for r in rooms]
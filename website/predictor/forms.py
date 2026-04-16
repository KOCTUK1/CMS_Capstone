"""
Selects day of the week and month
"""

from django import forms
from .services import DAY_CHOICES, MONTH_CHOICES


class ForecastForm(forms.Form):
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
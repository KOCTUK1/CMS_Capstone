"""
Django command to train model
"""

from django.core.management.base import BaseCommand
from predictor.ml.room_predictor import train
import os


class Command(BaseCommand):
    help = "Train prediction model"

    def handle(self, *args, **options):
        data_path = os.path.join(
            os.path.dirname(__file__),  # management/commands/
            "..", "..",                  
            "ml", "cleaned_data.csv",
        )
        data_path = os.path.abspath(data_path)

        self.stdout.write(f"Data from: {data_path}")
        train(data_path)
        self.stdout.write(self.style.SUCCESS("Model trained!"))
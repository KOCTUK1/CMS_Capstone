from django.apps import AppConfig


class PredictorConfig(AppConfig):
    name = 'predictor'

    def ready(self):
        import os
        if os.environ.get("PREDICTOR_SKIP_MODEL_LOAD") == "1":
            return
        try:
            from . import services
            services._load_model_and_encoders()
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                "Predictor model not loaded at startup: %s", exc
            )
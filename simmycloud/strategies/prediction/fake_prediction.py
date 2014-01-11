
from core.strategies import PredictionStrategy

class FakePrediction(PredictionStrategy):

    @PredictionStrategy.predict_strategy
    def predict(self, vm):
        return None

import numpy as np

class WeightedEnsemble:
    def __init__(self, weights):
        self.weights = np.array(weights)
        if not np.isclose(self.weights.sum(), 1.0):
            raise ValueError("Weights must sum to 1.")

    def combine(self, probas: list[np.ndarray]) -> np.ndarray:
        """
        probas: [gpt_proba, kfdeberta_proba, lightgbm_proba]
        """
        probas = np.array(probas)
        weighted = np.sum(self.weights[:, None] * probas, axis=0)
        return weighted

    def predict(self, probas: list[np.ndarray]) -> int:
        combined = self.combine(probas)
        return int(np.argmax(combined))
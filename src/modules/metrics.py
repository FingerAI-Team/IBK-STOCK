import numpy as np
import evaluate

class ClassificationMetricModule:
    def __init__(self):
        self.metric = evaluate.load("accuracy")

    def compute(self, eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        return self.metric.compute(
            predictions=predictions,
            references=labels
        )
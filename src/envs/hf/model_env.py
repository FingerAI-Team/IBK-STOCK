from transformers import AutoModelForSequenceClassification
from src.modules.label_mapper import ID2LABEL, LABEL2ID
from src.config import ModelConfig
import torch

class HFModelEnv:
    def __init__(self, config: ModelConfig | None=None):
        self.config = config
        self._model = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    @property
    def model(self):
        if self._model is None:
            self._model = AutoModelForSequenceClassification.from_pretrained(
                self.config.model_path,
                num_labels=2,
                id2label=ID2LABEL,
                label2id=LABEL2ID
            )
            self._model.to(self.device)
        return self._model

    def set_eval(self):
        self.model.eval()

    def set_train(self):
        self.model.train()
        
    def save(self, save_path: str):
        self.model.save_pretrained(save_path)
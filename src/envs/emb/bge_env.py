from src.config import BGEConfig
import torch 

class BGEModel():
    def __init__(self, model_name="BAAI/bge-m3", config: BGEConfig | None = None):
        self.model_name = model_name
        self._model = None
        self.config = config or BGEConfig()
        self.device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

    @property
    def model(self):
        if self._model is None:
            from FlagEmbedding import BGEM3FlagModel
            self._model = BGEM3FlagModel(self.model_name, use_fp16=True)
        return self._model

    def encode(self, text: str):
        result = self.model.encode(
            text,
            batch_size=self.config.batch_size,
            max_length=self.config.max_length
        )
        return result["dense_vecs"]
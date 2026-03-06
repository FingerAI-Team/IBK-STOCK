from src.modules.label_mapper import ID2LABEL
from src.envs.hf.model_env import HFModelEnv
from src.envs.hf.tokenizer_env import HFTokenizerEnv
import torch.nn.functional as F
import torch

class HFInferenceEnv:
    def __init__(self, model_env: HFModelEnv, tokenizer_env: HFTokenizerEnv):
        self.model_env = model_env
        self.tokenizer_env = tokenizer_env

    def predict(self, text: str) -> str:
        self.model_env.set_eval()
        inputs = self.tokenizer_env.tokenizer(text, return_tensors="pt")
        
        with torch.no_grad():
            outputs = self.model_env.model(**inputs)
            prediction = torch.argmax(outputs.logits, dim=1).item()
            print(prediction)
        return ID2LABEL[prediction]

    def predict_proba(self, text: str):
        self.model_env.model.eval()
        inputs = self.tokenizer_env.tokenizer(
            text,
            truncation=True,
            return_tensors="pt"
        )
        inputs = {
            k: v.to(self.model_env.device)
            for k, v in inputs.items()
        }
        with torch.no_grad():
            outputs = self.model_env.model(**inputs)
            probs = F.softmax(outputs.logits, dim=-1)[0]
        return probs.tolist()
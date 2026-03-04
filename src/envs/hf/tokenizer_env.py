from transformers import AutoTokenizer
import torch

class HFTokenizerEnv:
    def __init__(self, tokenizer_path: str):
        self.tokenizer_path = tokenizer_path
        self._tokenizer = None  # lazy 대상
        self.device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            self._tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_path)
        return self._tokenizer

    def tokenize_text(self, text: str):
        return self.tokenizer.tokenize(text)

    def tokenize_batch(self, texts, return_tensors="pt"):
        return self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            return_tensors=return_tensors
        )

    def save(self, save_path: str):
        self.tokenizer.save_pretrained(save_path)
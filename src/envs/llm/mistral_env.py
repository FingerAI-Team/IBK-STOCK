from transformers import AutoTokenizer, AutoModelForCausalLM, GenerationConfig
from src.config import MistralConfig
import torch
import os


class MistralEnv:
    def __init__(self, config: MistralConfig | None=None):
        self.config = config or MistralConfig()
        self._model = None
        self._tokenizer = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            self._tokenizer = AutoTokenizer.from_pretrained(
                os.path.join(self.config.model_path, self.config.model_type, "tokenizer")
            )
        return self._tokenizer

    @property
    def model(self):
        if self._model is None:
            self._model = AutoModelForCausalLM.from_pretrained(
                os.path.join(self.config.model_path, self.config.model_type),
                torch_dtype=torch.float16,
                low_cpu_mem_usage=True,
            ).to(self.device)
        return self._model

    @property
    def generation_config(self):
        return GenerationConfig(
            temperature=self.config.temperature,
            do_sample=self.config.do_sample,
            top_p=self.config.top_p,
            max_new_tokens=self.config.max_new_tokens,
        )

    def generate(self, prompt: str) -> str:
        self.model.eval()
        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            return_token_type_ids=False
        ).to(self.device)
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                generation_config=self.generation_config,
                pad_token_id=self.tokenizer.eos_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
            )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=False)
        return self._extract_response(decoded)

    def _extract_response(self, text: str) -> str:
        start_tag = "[/INST]"
        idx = text.find(start_tag)
        if idx != -1:
            return text[idx + len(start_tag):].strip()
        return text.strip()
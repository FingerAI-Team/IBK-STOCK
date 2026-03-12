class LlamaEnv(BaseLLMEnv):
    def __init__(self, config):
        super().__init__(config)
        self._pipeline = None

    @property
    def pipeline(self):
        if self._pipeline is None:
            self._pipeline = transformers.pipeline(
                "text-generation",
                model=self.config.model_name,
                torch_dtype=torch.float16,
                device_map="auto",
            )
        return self._pipeline

    def generate(self, messages):
        prompt = self._build_prompt(messages)
        outputs = self.pipeline(
            prompt,
            max_new_tokens=self.config.max_tokens,
            temperature=self.config.temperature
        )
        return outputs[0]["generated_text"]
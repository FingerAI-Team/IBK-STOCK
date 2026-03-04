from openai import OpenAI

class OpenAIEnv:
    def __init__(self, config):
        super().__init__(config)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = OpenAI(api_key=self.config.api_key)
        return self._client

    def generate(self, messages):
        response = self.client.chat.completions.create(
            model=self.config.model_name,
            messages=messages,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
        )
        return response.choices[0].message.content
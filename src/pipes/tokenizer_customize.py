from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.model_env import HFModelEnv
from src.config import TokenizerConfig, ModelConfig, ClassifyConfig
import pandas as pd 

class TokenizerCustomizePipe:
    def __init__(self, tokenizer_config: TokenizerConfig | None = None, model_config: ModelConfig | None = None, tickle_config: ClassifyConfig | None = None):
        self.tokenizer_config = tokenizer_config or TokenizerConfig()
        self.model_config = model_config or ModelConfig()
        self.tokenizer_env = HFTokenizerEnv()
        self.model_env = HFModelEnv()        
        self.classify_config = tickle_config or ClassifyConfig()
        self.tokenizer = self.tokenizer_env.tokenizer
        self.model = self.model_env.model
        self._tickle_set = None
        
    @property
    def tickle_set(self):
        if self._tickle_set is None:
            self._tickle_set = set(self._load_tickle_set())
        return self._tickle_set

    def _load_tickle_set(self):
        tickles = pd.read_csv(self.classify_config.tickle_set_path)
        tickles.dropna(inplace=True)
        return tickles["tickle"].values.tolist()

    def _save_models(self):
        self.tokenizer_env.save(self.tokenizer_config.tokenizer_path)
        self.model_env.save_model(self.model_config.save_model_path)

    def run(self):
        added = self.tokenizer.add_tokens(list(self.tickle_set))
        if added > 0:
            print(f"Added {added} tokens to tokenizer")

        emb_size = self.model.get_input_embeddings().num_embeddings
        tok_size = len(self.tokenizer)
        if emb_size != tok_size:
            print(f"Resizing embeddings {emb_size} -> {tok_size}")
            self.model.resize_token_embeddings(tok_size)
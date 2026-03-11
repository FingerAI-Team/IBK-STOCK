from src.pipes.train_pipe import ModelTrainer
from src.pipes.db_pipe import DBPipe
from src.envs.hf.model_env import HFModelEnv
from src.envs.hf.tokenizer_env import HFTokenizerEnv


class IBKModelTrainer:
    def __init__(self, model_env: HFModelEnv, tokenizer_env: HFTokenizerEnv, db_pipe: DBPipe, trainer: ModelTrainer):
        self.model_env = model_env
        self.tokenizer_env = tokenizer_env
        self.db_pipe = db_pipe
        self.trainer = trainer(model_env, tokenizer_env)


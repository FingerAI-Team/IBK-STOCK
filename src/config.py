from dataclasses import dataclass

@dataclass(frozen=True)
class BGEConfig:
    batch_size: int = 12
    max_length: int = 1024    

@dataclass(frozen=True)
class ModelConfig:
    model_paht: str = "/stock-service/model/kfdeberta/model-update"
    save_model_path: str = "/stock-service/model/kfdeberta/model-update"
    model_type: str = "mistral"

@dataclass(frozen=True)
class ClassifyConfig:
    tickle_set_path: str = "/stock-service/tickle/tickle-final.csv"

@dataclass(frozen=True)
class TokenizerConfig:
    tokenizer_path: str = "/stock-service/model/val-tokenizer"

@dataclass(frozen=True)
class HFTrainConfig:
    output_subdir: str = "kfdeberta"
    learning_rate: float = 2e-5
    train_batch_size: int = 16
    eval_batch_size: int = 16
    num_train_epochs: int = 10
    weight_decay: float = 0.01
    eval_strategy: str = "no"
    save_strategy: str = "no"
    eval_steps: int = 1000
    save_steps: int = 1000
    load_best_model_at_end: bool = True
    metric_for_best_model: str = "eval_loss"

@dataclass(frozen=True)
class MistralConfig:
    model_path: str
    model_type: str
    temperature: float = 0.8
    top_p: float = 0.95
    do_sample: bool = True
    max_new_tokens: int = 512

@dataclass(frozen=True)
class dataConfig:
    random_state = 42
    test_size = 0.2
    val_test_size = 0.2

@dataclass(frozen=True)
class OnelineConfig:
    bearer_token: str = ""
    base_url: str = "https://chat-api.ibks.onelineai.com"

@dataclass(frozen=True)
class DBConfig:   
    host: str = "postgres_postgresql-master_1"
    db_name: str = "ibk_db"
    user_id: str = "ibk-manager"
    user_pw: str = "fingerai2024!"
    port: int = 5432
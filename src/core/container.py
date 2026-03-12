from src.pipes.classify_pipe import DataClassifier
from src.pipes.transform_pipe import TransformPipe
from src.pipes.collect_pipe import DataCollector
from src.pipes.train_pipe import ModelTrainer
from src.pipes.store_pipe import StorePipe
from src.pipes.db_pipe import DBPipe
from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.model_env import HFModelEnv
from src.envs.collect_api import IBKAPIEnv 
from src.services.ibk_train import IBKModelTrainer
from src.services.ibk_service import IBKPipeline
from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository
from src.repositories.core import DBConnection
from src.config import DBConfig, OnelineConfig
from src.config import OnelineConfig

def build_container(oneline_config: OnelineConfig):
    # Envs 
    hf_env = HFModelEnv()
    tokenizer_env = HFTokenizerEnv()
    collect_env = IBKAPIEnv(config=oneline_config)
    db_connection = DBConnection(DBConfig)
    db_connection.connect() 
    conv_repo = ConvRepository(db_connection.conn)
    cls_repo = ClsRepository(db_connection.conn)

    # Pipe 
    db_pipe = DBPipe()
    collect_pipe = DataCollector(api_env=collect_env)
    transform_pipe = TransformPipe(conv_repo=conv_repo)
    classify_pipe = DataClassifier()
    store_pipe = StorePipe(conv_repo=conv_repo, cls_repo=cls_repo)
    trainer_pipe = ModelTrainer(tokenizer_env, hf_env)

    # Service
    train_service = IBKModelTrainer(model_env=hf_env, tokenizer_env=tokenizer_env, db_pipe=db_pipe, trainer=trainer_pipe)
    ibk_service = IBKPipeline(api_env=collect_env, conv_repo=conv_repo, cls_repo=cls_repo, 
                              collect_pipe=collect_pipe, transform_pipe=transform_pipe, classify_pipe=classify_pipe, store_pipe=store_pipe)
    return {
        "train_service": train_service, 
        "ibk_service": ibk_service
    }
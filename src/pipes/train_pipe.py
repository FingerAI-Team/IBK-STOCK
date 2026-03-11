from src.modules.processors.dataframe_utils import data_to_df, merge_data, filter_data, train_test_split
from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.model_env import HFModelEnv
from src.envs.hf.predictor_env import HFInferenceEnv
from datasets import Dataset, DatasetDict

class ModelTrainer:
    def __init__(
        self,
        tokenizer_env: HFTokenizerEnv,
        model_env: HFModelEnv,
        predictor_env: HFInferenceEnv
    ):
        self.tokenizer_env = tokenizer_env
        self.model_env = model_env
        self.predictor_env = predictor_env

    def _set_cls_trainset(self, convlog_data, cls_data):
        '''
        db에서 입력받은 데이터를 학습 데이터세트로 변환합니다. 
        '''
        conv_df = data_to_df(convlog_data, columns=['conv_id', 'date', 'qa', 'content', 'userid'])
        cls_df = data_to_df(cls_data, columns=['conv_id', 'ensemble', 'gpt', 'encoder']) 
        convlog_q = filter_data(conv_df, 'qa', 'Q')
        convlog_trainset = merge_data(convlog_q, cls_df, on='conv_id')
        convlog_trainset['label'] = convlog_trainset['ensemble'].apply(lambda x: 'stock' if x == 'o' else 'nstock')
        convlog_trainset = convlog_trainset[['content', 'label']]
        X_train, X_val, X_test, y_train, y_val, y_test = train_test_split(convlog_trainset, 'content', 'label', \
                                                                    0.2, 0.1, self.model_config['random_state'])
        train_df = data_to_df(list(zip(X_train, y_train)), columns=['text', 'label']).reset_index(drop=True)
        val_df = data_to_df(list(zip(X_val, y_val)), columns=['text', 'label']).reset_index(drop=True)
        test_df = data_to_df(list(zip(X_test, y_test)), columns=['text', 'label']).reset_index(drop=True)
        print(len(train_df), len(val_df), len(test_df))
        stock_dict = DatasetDict({
            "train": Dataset.from_pandas(train_df), 
            "val": Dataset.from_pandas(val_df),
            "test": Dataset.from_pandas(test_df)
        })
        return stock_dict
    
    

    def train(self, train_data, val_data):
        tokenizer = self.tokenizer_env.load_tokenizer()
        model = self.model_env.load_model()
        model.fit(train_data)
        eval_results = model.evaluate(val_data)
        print(f"Evaluation results: {eval_results}")
        self.model_env.save_model(model)
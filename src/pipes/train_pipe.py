from src.modules.processors.dataframe_utils import data_to_df, merge_data, filter_data, train_test_split
from src.modules.metrics import ClassificationMetricModule
from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.model_env import HFModelEnv
from src.config import HFTrainConfig
from transformers import Trainer, DataCollatorWithPadding
from transformers import TrainingArguments
from datasets import Dataset, DatasetDict


class ModelTrainer:
    def __init__(self, tokenizer_env: HFTokenizerEnv, model_env: HFModelEnv, 
                 metric_module: ClassificationMetricModule | None = None, train_config: HFTrainConfig | None=None):
        self.tokenizer_env = tokenizer_env
        self.model_env = model_env
        self.metric_module = metric_module or ClassificationMetricModule()
        self.train_config = train_config or HFTrainConfig()
    
    def _build_dataset(self, conv_data, cls_data):
        '''
        db에서 입력받은 데이터를 학습 데이터세트로 변환합니다. 
        '''
        convlog_data = data_to_df(conv_data, columns=['conv_id', 'date', 'qa', 'content', 'userid'])
        cls_data = data_to_df(cls_data, columns=['conv_id', 'ensemble', 'gpt', 'encoder']) 
        convlog_q = filter_data(convlog_data, 'qa', 'Q')
        convlog_trainset = merge_data(convlog_q, cls_data, on='conv_id')
        convlog_trainset['label'] = convlog_trainset['ensemble'].apply(lambda x: 'stock' if x == 'o' else 'nstock')
        label_map = {'stock': 0, 'nstock': 1}
        convlog_trainset['label'] = convlog_trainset['label'].map(label_map)
        convlog_trainset = convlog_trainset[['content', 'label']]
        X_train, X_val, X_test, y_train, y_val, y_test = train_test_split(convlog_trainset, 'content', 'label')
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
    
    def _build_training_args(self):
        return TrainingArguments(
            output_dir=self.train_config.save_model_path,
            learning_rate=self.train_config.learning_rate,
            per_device_train_batch_size=self.train_config.train_batch_size,
            per_device_eval_batch_size=self.train_config.eval_batch_size,
            num_train_epochs=self.train_config.num_train_epochs,
            weight_decay=self.train_config.weight_decay,
            evaluation_strategy=self.train_config.eval_strategy,
            save_strategy=self.train_config.save_strategy,
            eval_steps=self.train_config.eval_steps,
            save_steps=self.train_config.save_steps,
            load_best_model_at_end=self.train_config.load_best_model_at_end,
            metric_for_best_model=self.train_config.metric_for_best_model,
            push_to_hub=False,
        )

    def _tokenize_dataset(self, dataset):
        def tokenize_function(examples):
            return self.tokenizer_env.tokenizer(
                examples["text"],
                truncation=True,
                max_length=self.train_config.max_length
            )
        tokenized_dataset = dataset.map(tokenize_function, batched=True)
        tokenized_dataset = tokenized_dataset.remove_columns(["text"])
        tokenized_dataset.set_format(
            type="torch",
            columns=["input_ids", "attention_mask", "label"]
        )
        return tokenized_dataset

    def _build_trainer(self, dataset):
        data_collator = DataCollatorWithPadding(
            tokenizer=self.tokenizer_env.tokenizer,
            padding=True
        )
        return Trainer(
            model=self.model_env.model,
            tokenizer=self.tokenizer_env.tokenizer,
            args=self._build_training_args(),
            train_dataset=dataset["train"],
            eval_dataset=dataset["val"],
            data_collator=data_collator,
            compute_metrics=self.metric_module.compute,
        )
    
    def train(self, dataset):
        trainer = self._build_trainer(dataset)
        trainer.train()
        return trainer

    def save_model(self, trainer, model_path):
        trainer.save_model(model_path)

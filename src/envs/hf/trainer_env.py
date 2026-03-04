from transformers import Trainer, DataCollatorWithPadding
from transformers import TrainingArguments
import numpy as np
import evaluate
import os


class HFTrainerEnv:
    def __init__(self, model_env, tokenizer_env, train_config, base_model_path):
        self.model_env = model_env
        self.tokenizer_env = tokenizer_env
        self.train_config = train_config
        self.base_model_path = base_model_path
        self._accuracy = None 
    
    @property
    def accuracy(self):
        if self._accuracy is None:
            self._accuracy = evaluate.load("accuracy")
        return self._accuracy
    
    def compute_metrics(self, eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        return self.accuracy.compute(
            predictions=predictions,
            references=labels
        )

    def build_training_args(self):
        return TrainingArguments(
            output_dir=os.path.join(
                self.base_model_path,
                self.train_config.output_subdir
            ),
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

    def _build_trainer(self, dataset):
        data_collator = DataCollatorWithPadding(
            tokenizer=self.tokenizer_env.tokenizer,
            padding=True
        )
        return Trainer(
            model=self.model_env.model,
            args=self.build_training_args(),
            train_dataset=dataset["train"],
            eval_dataset=dataset["val"],
            data_collator=data_collator,
            compute_metrics=self.compute_metrics,
        )

    def train(self, dataset):
        trainer = self._build_trainer(dataset)
        trainer.train()
        return trainer

    def save_model(self, trainer, model_path):
        trainer.save_model(model_path)
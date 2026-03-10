from src.modules.processors.text_cleaner import remove_patterns
from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.predictor_env import HFInferenceEnv
from src.envs.hf.model_env import HFModelEnv
from src.config import ClassifyConfig
import pandas as pd 

'''Classification Pipe'''
class DataClassifier:
    def __init__(self, tickle_config: ClassifyConfig | None = None):
        self.model_env = HFModelEnv()
        self.tokenizer_env = HFTokenizerEnv()
        tokenizer = self.tokenizer_env.tokenizer
        model = self.model_env.model
        self.classify_config = tickle_config or ClassifyConfig()
        emb_size = model.get_input_embeddings().num_embeddings
        if emb_size != len(tokenizer):
            print(f"Resizing model embeddings from {emb_size} to {len(tokenizer)}")
            model.resize_token_embeddings(len(tokenizer))

        self._tickle_set = None
        self.predictor_env = HFInferenceEnv(
            model_env=self.model_env,
            tokenizer_env=self.tokenizer_env
        )

    @property
    def tickle_set(self):
        if self._tickle_set is None:
            self._tickle_set = set(self._load_tickle_set())
        return self._tickle_set

    def _load_tickle_set(self):
        tickles = pd.read_csv(self.classify_config.tickle_set_path)
        tickles.dropna(inplace=True)
        return tickles["tickle"].values.tolist()

    def filter_text(self, text: str) -> str | None:
        """
        단일 토큰 질문일 경우 빠른 종목 필터
        반환:
            'o' / 'x' → override
            None → encoder 판단 사용
        """
        # print(f'filter_text input: {text}')
        if len(self.tokenizer_env.tokenize_text(text)) == 1:
            cleaned_word = remove_patterns(
                text, r"(뉴스|주식|정보|분석)$"
            )
            return 'stock' if cleaned_word in self.tickle_set else 'nstock'
        return None

    def run(self, records):
        cls_rows = []
        texts = []
        conv_ids = []
        for record in records:
            if record["qa"] != "Q":
                continue
            text = record["content"]
            if not text:
                continue

            fast_filter = self.filter_text(text)
            if fast_filter is None:
                texts.append(text)
                conv_ids.append(record["conv_id"])
            else:
                enc_res = 'o' if fast_filter == 'stock' else 'x'
                cls_rows.append((record["conv_id"], enc_res))
        if texts:
            preds = self.predictor_env.predict_batch(texts)
            for conv_id, pred in zip(conv_ids, preds):
                enc_res = 'o' if pred == 'stock' else 'x'
                cls_rows.append((conv_id, enc_res))
        return cls_rows
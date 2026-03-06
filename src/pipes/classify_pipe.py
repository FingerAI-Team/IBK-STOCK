from src.envs.hf.tokenizer_env import HFTokenizerEnv
from src.envs.hf.model_env import HFModelEnv
from src.envs.hf.predictor_env import HFInferenceEnv
from src.modules.processors.text_cleaner import remove_patterns

'''Classification Pipe'''
class DataClassifier:
    def __init__(self):
        self.model_env = HFModelEnv()
        self.tokenizer_env = HFTokenizerEnv()
        self.predictor_env = HFInferenceEnv(model_env=self.model_env, tokenizer_env=self.tokenizer_env)  
    
    def filter_text(self, text: str) -> str | None:
        """
        단일 토큰 질문일 경우 빠른 종목 필터
        반환:
            'o' / 'x' → override
            None      → encoder 판단 사용
        """
        print(f'filter_text input: {text}')
        if len(self.tokenizer_env.tokenize_text(text)) == 1:
            cleaned_word = remove_patterns(
                text, r"(뉴스|주식|정보|분석)$"
            )
            return 'stock' if cleaned_word in self.tickle_set else 'nstock'
        return None

    def run(self, records):
        cls_rows = []
        for record in records:
            try:
                if record["qa"] != "Q":
                    continue
                text = record["content"]
                if not text:
                    continue
                stock_pred = self.predictor_env.predict(text)
                print(f'stock_pred: {stock_pred}, text: {text}')
                fast_filter = self.filter_text(text)
                print('fast_filter: ', fast_filter)
                if fast_filter is not None:
                    stock_pred = fast_filter

                enc_res = 'o' if stock_pred == 'stock' else 'x'
                conv_id = record["conv_id"]
                cls_rows.append((conv_id, enc_res))
            except: 
                print(text)
        print(f'cls_rows: {cls_rows}')
        return cls_rows 
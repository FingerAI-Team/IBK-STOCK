from src.pipes.collect_pipe import DataCollector
from src.envs.collect_api import IBKAPIEnv
from src.core.settings import ONELINE_CONFIG

ibk_api_env = IBKAPIEnv(config=ONELINE_CONFIG)
data_collector = DataCollector(ibk_api_env)

if __name__ == "__main__":
    day_logs = data_collector.run()
    print(day_logs)
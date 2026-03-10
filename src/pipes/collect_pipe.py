from src.envs.collect_api import IBKAPIEnv 
from datetime import datetime, timedelta

class DataCollector:
    def __init__(self, api_env: IBKAPIEnv):
        self.api_env = api_env

    def collect(self, start_date, to_date):
        tenant_id = ["ibk", "ibks"]
        all_logs = []
        for tenant in tenant_id:
            logs = self.api_env.fetch_logs(start_date, to_date, tenant)
            all_logs.extend(logs)
        return all_logs
    
    def run(self, start_date=None, end_date=None):
        if start_date is None:
            now = datetime.now()
            start_date = now.strftime("%Y-%m-%d")
        if end_date is None:
            end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        day_logs = self.collect(start_date, end_date)
        return day_logs
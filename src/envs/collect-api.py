from src.config import OnelineConfig
import requests
import hashlib

class IBKAPIEnv:
    def __init__(self, config: OnelineConfig | None=None):
        self.config = config or OnelineConfig()
        self.bearer_token = self.config.bearer_token
        self.session = requests.Session()

    def fetch_logs(self, from_date, to_date, tenant_id="ibk"):
        url = (
            self.config.base_url + 
            f"/api/ibk_securities/admin/logs?tenant_id={tenant_id}"
            f"&from_date_utc={from_date}&to_date_utc={to_date}"
        )
        headers = {
            "Authorization": f"Bearer {self.bearer_token}"
        }
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"IBK API request failed: {e}")
import requests
from dataclasses import dataclass, field


@dataclass
class BrawlStarsClient:
    api_key: str
    base_url: str
    headers: dict = field(init=False)

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def get_player(self, player_tag: str) -> dict:
        url = f"{self.base_url}/players/{player_tag}"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

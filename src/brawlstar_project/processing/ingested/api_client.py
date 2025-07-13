from dataclasses import dataclass, field

import requests


@dataclass
class BrawlStarsClient:
    api_key: str
    base_url: str
    headers: dict = field(init=False)

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def _get(self, path: str) -> dict:
        """
        Internal helper to perform GET requests.

        Args:
            path: URL path (starting without /)

        Returns:
            Parsed JSON response as a Python dictionary.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def get_player(self, player_tag: str) -> dict:
        return self._get(f"players/{player_tag}")

    def get_battlelog(self, player_tag: str) -> dict:
        return self._get(f"players/{player_tag}/battlelog")

    def get_club(self, club_tag: str) -> dict:
        return self._get(f"clubs/{club_tag}")

    def get_club_members(self, club_tag: str) -> dict:
        return self._get(f"clubs/{club_tag}/members")

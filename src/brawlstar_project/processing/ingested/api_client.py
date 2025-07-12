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

    def get_player(self, player_tag: str) -> dict:
        """
        Fetch player information from the Brawl Stars API.

        Args:
            player_tag: Brawl Stars player tag (without #)

        Returns:
            Parsed JSON response as a Python dictionary.
        """
        url = f"{self.base_url}/players/{player_tag}"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def get_battlelog(self, player_tag: str) -> dict:
        """
        Fetch the battle log for a given player from the Brawl Stars API.

        Args:
            player_tag: Brawl Stars player tag (without #)

        Returns:
            Parsed JSON response as a Python dictionary.
        """
        url = f"{self.base_url}/players/{player_tag}/battlelog"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def get_club(self, club_tag: str) -> dict:
        """
        Fetch club information from the Brawl Stars API.

        Args:
            club_tag: Brawl Stars club tag (without #)

        Returns:
            Parsed JSON response as a Python dictionary.
        """
        url = f"{self.base_url}/clubs/{club_tag}"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def get_club_members(self, club_tag: str) -> dict:
        """
        Fetch the list of members of a given club.

        Args:
            club_tag: Brawl Stars club tag (with or without '#')

        Returns:
            Parsed JSON response as dict.
        """
        url = f"{self.base_url}/clubs/{club_tag}/members"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

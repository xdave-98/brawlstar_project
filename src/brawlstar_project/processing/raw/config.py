import os
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class ConfigLoader:
    api_key: str
    player_tag: str
    base_url: str = "https://api.brawlstars.com/v1/"

    @classmethod
    def from_env(cls, env_file: str = ".env"):
        load_dotenv(dotenv_path=env_file)

        api_key = os.getenv("BRAWLSTARS_API_KEY")
        player_tag = os.getenv("BRAWLSTARS_PLAYER_TAG")
        base_url = os.getenv("BRAWLSTARS_BASE_URL", "https://api.brawlstars.com/v1/")

        config = cls(api_key, player_tag=player_tag, base_url=base_url)  # type: ignore
        config.validate()
        return config

    def validate(self):
        if not self.api_key:
            raise ValueError("BRAWLSTARS_API_KEY is missing in .env")
        if not self.player_tag:
            raise ValueError("BRAWLSTARS_PLAYER_TAG is missing in .env")

    @property
    def formatted_player_tag(self) -> str:
        return self.player_tag.replace("#", "%23")

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class ConfigLoader:
    api_key: str
    base_url: str = "https://api.brawlstars.com/v1/"

    @classmethod
    def from_env(cls, env_file: str = ".env"):
        load_dotenv(dotenv_path=env_file)

        api_key = os.getenv("BRAWLSTARS_API_KEY")
        base_url = os.getenv("BRAWLSTARS_BASE_URL", "https://api.brawlstars.com/v1/")

        config = cls(api_key, base_url=base_url)  # type: ignore
        config.validate()
        return config

    def validate(self):
        if not self.api_key:
            raise ValueError("BRAWLSTARS_API_KEY is missing in .env")

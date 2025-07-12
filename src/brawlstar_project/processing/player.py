from dataclasses import dataclass
import re

@dataclass
class Player:
    tag: str

    def __post_init__(self):
        if not self.tag.startswith("#"):
            self.tag = "#" + self.tag

        player_tag = self.tag[1:]

        # Check if player tag contains 8 caracters
        if len(player_tag) != 8:
            raise ValueError("Player tag must be exactly 8 characters after '#'")

        # Player tag should only contains letters and numbers
        if not re.fullmatch(r"[A-Z0-9]{8}", player_tag):
            raise ValueError("Player tag must contain only uppercase letters A-Z and digits 0-9")

    @property
    def formatted_tag(self) -> str:
        return self.tag.replace("#", "%23")

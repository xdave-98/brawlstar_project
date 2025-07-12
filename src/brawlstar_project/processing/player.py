from dataclasses import dataclass


@dataclass
class Player:
    tag: str

    def __post_init__(self):
        if not self.tag.startswith("#"):
            self.tag = "#" + self.tag

    @property
    def formatted_tag(self) -> str:
        """
        Returns the tag formatted for use in the API URL
        (replace '#' with '%23').
        """
        return self.tag.replace("#", "%23")

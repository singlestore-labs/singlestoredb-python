import configparser
from typing import Any


class Parser(configparser.RawConfigParser):

    def __init__(self, **kwargs: Any) -> None:
        kwargs['allow_no_value'] = True
        configparser.RawConfigParser.__init__(self, **kwargs)

    def __remove_quotes(self, value: str) -> str:
        quotes = ["'", '"']
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value

    def get(self, section: str, option: str) -> str:  # type: ignore
        value = configparser.RawConfigParser.get(self, section, option)
        return self.__remove_quotes(value)

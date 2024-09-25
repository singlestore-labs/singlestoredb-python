from dataclasses import dataclass


@dataclass
class ConnectionInfo:
    url: str
    token: str

from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionInfo:
    url: str

    # Only present in interactive mode
    token: Optional[str]

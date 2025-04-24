from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional


@dataclass
class ConnectionInfo:
    url: str

    # Only present in interactive mode
    token: Optional[str]


@dataclass
class UdfConnectionInfo:
    url: str
    functions: Dict[str, Any]

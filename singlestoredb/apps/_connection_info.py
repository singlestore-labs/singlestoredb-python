from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class ConnectionInfo:
    url: str

    # Only present in interactive mode
    token: Optional[str]

@dataclass
class PythonUdfConnectionInfo:
    url: str
    functions: Dict[str, Any] 

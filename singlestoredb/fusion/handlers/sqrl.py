import requests
import os

from typing import Any
from typing import Dict
from typing import Optional

from singlestoredb.fusion.handler import SQLHandler
from singlestoredb.fusion.result import FusionSQLResult

class SqrlExplain(SQLHandler):
    """
    SQRL EXPLAIN string_value;

    string_value = '<string-value>'

    """
    INKEEP_API_KEY = os.getenv('INKEEP_API_KEY')
    INKEEP_INTEGRATION_ID = os.environ.get('INKEEP_INTEGRATION_ID')

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        prompt = "Explain me this SQL query:" + params['string_value']
        apiUrl = "https://api.inkeep.com/v0/chat_sessions/chat_results"

        resp=  requests.post(
            apiUrl,
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + INKEEP_API_KEY,
            },
            json={
                "integration_id": INKEEP_INTEGRATION_ID,
                "chat_session": {
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                },
            "stream": False
            },
        ) 
 
        if resp.status_code != 200:
            raise ValueError(f'an error occurred: {res.text}')
        
        data = resp.json()
        content = data["message"]["content"]
        print(content)

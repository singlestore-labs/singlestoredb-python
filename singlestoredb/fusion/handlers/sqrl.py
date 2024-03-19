import requests

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

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        prompt = "Explain me this SQL query:" + params['string_value']
        apiUrl = "https://api.inkeep.com/v0/chat_sessions/chat_results"
        
        resp=  requests.post(apiUrl,
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + env.inkeepAPIKey,
        },
        json={
            "integration_id": env.inkeepIntegrationId,
            "chat_session": {
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
               },
           "stream": False
        },) 
 
        if resp.status_code != 200:
            raise ValueError(f'an error occurred: {res.text}')
        
        data = resp.json()
        content = data["message"]["content"]
        print(content)

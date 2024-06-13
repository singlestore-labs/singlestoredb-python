import os as _os
from typing import Any

try:
    from langchain_community.embeddings.ollama import OllamaEmbeddings
except ImportError:
    raise ImportError(
        'Could not import langchain_community python package. '
        'Please install it with `pip install langchain_community`.',
    )


class SingleStoreEmbeddings(OllamaEmbeddings):

    def __init__(self, **kwargs: Any):
        url = _os.getenv('SINGLESTORE_AI_EXPERIMENTAL_URL')
        if not url:
            raise ValueError(
                "Environment variable 'SINGLESTORE_AI_EXPERIMENTAL_URL' must be set",
            )

        base_url = url.strip('/v1')
        kwargs = {'model': 'nomic-embed-text', **kwargs}
        super().__init__(base_url=base_url, **kwargs)

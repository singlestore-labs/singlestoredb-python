import logging

import httpx

logger = logging.getLogger(__name__)


class SingleStoreOpenAIAuth(httpx.Auth):
    def __init__(self, obo_token_getter):  # type: ignore[no-untyped-def]
        self.obo_token_getter_fn = obo_token_getter

    def auth_flow(self, request: httpx.Request):  # type: ignore[no-untyped-def]
        logger.info(f'auth_flow called for request to {request.url}')
        if self.obo_token_getter_fn is not None:
            logger.debug('obo_token_getter_fn is set, attempting to get token')
            obo_val = self.obo_token_getter_fn()
            if obo_val:
                logger.info('OBO token retrieved successfully, adding X-S2-OBO header')
                request.headers['X-S2-OBO'] = obo_val
            else:
                logger.warning('obo_token_getter_fn returned empty/None value')
        else:
            logger.debug('obo_token_getter_fn is None, skipping OBO token')
        yield request

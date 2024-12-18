"""Loggable ``Retry()`` adapter for ``requests`` package"""

import logging

from urllib3 import Retry


class LoggingRetry(Retry):
    """In the case we need to throttle Coingecko or other HTTP API, be verbose about it."""

    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', logging.getLogger(__name__))
        super().__init__(*args, **kwargs)

    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        if response:
            status = response.status
            reason = response.reason
        else:
            status = None
            reason = str(error)

        url_shortened = url[0:96]

        self.logger.warning(f"Retrying: {method} {url_shortened} (status: {status}, reason: {reason})")
        return super().increment(method, url, response, error, _pool, _stacktrace)

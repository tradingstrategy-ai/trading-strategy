"""Loggable ``Retry()`` adapter for ``requests`` package"""

import logging

from urllib3 import Retry


class LoggingRetry(Retry):
    """In the case we need to throttle Coingecko or other HTTP API, be verbose about it.

    Example how to use:

    .. code-block:: python

        # Set up dealing with network connectivity flakey
        if retry_policy is None:
            # https://stackoverflow.com/a/35504626/315168
            retry_policy = LoggingRetry(
                total=5,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ],
            )
            session.mount('http://', HTTPAdapter(max_retries=retry_policy))
            session.mount('https://', HTTPAdapter(max_retries=retry_policy))
    """

    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', logging.getLogger(__name__))
        super().__init__(*args, **kwargs)

    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        if response:
            status = response.status
            reason = response.reason
            text = response.read().decode('utf-8', errors="ignore")
            headers = response.headers
        else:
            status = None
            reason = str(error)
            text = None
            headers = {}

        url_shortened = url[0:96]

        headers = [f"{k}: {v}" for k, v in headers.items()]
        headers_text = "\n".join(headers)
        self.logger.warning(f"Retrying: {method} {url_shortened} (status: {status}, reason: {reason}, text: {text})\nHeaders: {headers_text}")
        return super().increment(method, url, response, error, _pool, _stacktrace)

"""Python requests library downloads with a TQDM progress bar."""
import functools
import logging
import shutil
from typing import Optional

from requests import Session
from tqdm_loggable.auto import tqdm
from urllib3.exceptions import ProtocolError


logger = logging.getLogger(__name__)

def download_with_tqdm_progress_bar(
    session: Session,
    path: str,
    url: str,
    params: dict,
    timeout: float | tuple,
    human_readable_hint: Optional[str],
    attempts:int = 3,
):
    """Use tqdm library to raw a graphical progress bar in notebooks for long downloads.

    Autodetects the Python execution environment

    - Displays HTML progress bar in Jupyter notebooks

    - Displays ANSI progress bar in a console

    See :py:meth:`tradingstrategy.transport.cache.CachedHTTPTransport.save_response` for more information.

    :param timeout:
        Requests-style timeout (connection, read) tuple.

    :param attempts:
        Number of attempts before giving up.

    """
    # https://stackoverflow.com/questions/37573483/progress-bar-while-download-file-over-http-with-requests
    # https://stackoverflow.com/questions/42212810/tqdm-in-jupyter-notebook-prints-new-progress-bars-repeatedly

    r = session.get(url, stream=True, allow_redirects=True, params=params, timeout=timeout)
    if r.status_code != 200:
        try:
            r.raise_for_status()  # Will only raise for 4xx codes, so...
            raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
        except Exception as e:
            # Add more context information
            auth_key = session.headers.get("Authorization", "")[0:12]
            raise RuntimeError(f"Failed to do an API call, using API key {auth_key}...") from e

    file_size = int(r.headers.get('Content-Length', 0))

    desc = human_readable_hint or ""

    # Add warning about missing Content-Length header
    desc += " (Unknown total file size)" if file_size == 0 else ""

    attempt = 0
    while attempt < attempts:
        try:
            r.raw.read = functools.partial(r.raw.read, decode_content=True)  # Decompress if needed
            with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
                with open(path, "wb") as f:
                    shutil.copyfileobj(r_raw, f)

            return path
        except ProtocolError as e:
            # ProtocolError: ('Connection broken: IncompleteRead(20834 bytes read)', IncompleteRead(20834 bytes read))
            attempt += 1
            logger.warning(
                "download_with_tqdm_progress_bar(): Downloading %s. Retry %d / %d. Got error: %s",
                path,
                attempt + 1,
                attempts,
                str(e),
                exc_info=True,
            )
            if attempt >= attempts:
                raise



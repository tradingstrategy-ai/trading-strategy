import datetime
import logging
import time
from abc import ABC

from requests import Session

logger = logging.getLogger(__name__)


class Environment(ABC):
    """Capitalgram interacts within different run-time environments.

    User interactions is different in Jupyter (graphical CLI),
    console, in-page browser and when running in oracle sandbox.
    """


def download_with_progress_plain(session: Session, path: str, url: str, params: dict, timeout: float):
    """The default downloader does not display any fancy progress bar.

    :param timeout: Timeout in seconds.
    """

    start = datetime.datetime.utcnow()

    response = session.get(url, params=params, timeout=timeout)

    headers = response.headers
    if "content-length" in headers:
        human_size = "{:,}".format(int(headers["content-length"]))
    else:
        human_size = "unknown"
    logger.info(f"Downloading %s to path %s, size is %s bytes", url, path, human_size)

    fsize = 0
    with open(path, 'wb') as handle:
        for block in response.iter_content(8 * 1024):
            handle.write(block)
            fsize += len(block)
    duration = datetime.datetime.utcnow() - start
    logger.debug("Saved %s. Downloaded %d bytes in %s.", path, fsize, duration)

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


def download_with_progress_plain(session: Session, path: str, url: str, params: dict):
    """The default downloader does not display any fancy progress bar."""

    start = time.time()

    head = session.head(url)
    if "content-length" in head:
        human_size = "{:,}".format(head["content-length"])
    else:
        human_size = "unknown"
    logger.info("Downloading %s to path %s, size is %s", url, path, human_size)

    response = session.get(url, params=params)
    fsize = 0
    with open(path, 'wb') as handle:
        for block in response.iter_content(8 * 1024):
            handle.write(block)
            fsize += len(block)
    duration = time.time() - start
    logger.debug("Saved %s. Downloaded %s bytes in %f seconds.", path, fsize, duration)

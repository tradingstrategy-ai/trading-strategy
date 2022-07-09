import datetime
import logging
import time
from abc import ABC

from requests import Session, ReadTimeout

logger = logging.getLogger(__name__)


class Environment(ABC):
    """Capitalgram interacts within different run-time environments.

    User interactions is different in Jupyter (graphical CLI),
    console, in-page browser and when running in oracle sandbox.
    """


def download_with_progress_plain(session: Session, path: str, url: str, params: dict, timeout: float, human_desc: str):
    """The default downloader does not display any fancy progress bar.

    :param timeout: Timeout in seconds.
    """

    assert timeout > 0

    start = datetime.datetime.utcnow()

    logger.info("Starting %s download from %s", human_desc, url)

    # Work around the issue that HTTPS request get stuck on Github CI
    # https://github.com/tradingstrategy-ai/client/runs/5614200499?check_suite_focus=true
    attempts = max_attempts = 5
    response = None
    while attempts > 0:
        try:
            # https://docs.python-requests.org/en/v1.2.3/user/advanced/#body-content-workflow
            response = session.get(url, params=params, timeout=timeout, stream=True)
            break
        except ReadTimeout:
            attempts -= 1
            if attempts <= 0:
                raise
            logger.warning("Timeout when reading URL %s. Attempts left: %d, current timeout %f", url, attempts, timeout)
            time.sleep(timeout)
            timeout *= 2

    if attempts != max_attempts:
        logger.warning("Recovered download with attempts left: %d, %s", attempts, url)

    initial_response = datetime.datetime.utcnow() - start

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
            time_to_first_byte = datetime.datetime.utcnow() - start
    duration = datetime.datetime.utcnow() - start
    logger.info("Saved %s to %s. Downloaded %d bytes in %s. Time to initial response was: %s. Time to first byte was: %s.", url, path, fsize, duration, initial_response, time_to_first_byte)

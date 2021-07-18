from abc import ABC


class Environment(ABC):
    """Capitalgram interacts within different run-time environments.

    User interactions is different in Jupyter (graphical CLI),
    console, in-page browser and when running in oracle sandbox.
    """


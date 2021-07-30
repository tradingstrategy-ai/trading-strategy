import tempfile

from IPython.core.display import Image
from matplotlib.figure import Figure


def render_figure_in_docs(fig: Figure, dpi=450):
    """Work around various matplotlib rendering issues to get inline images displayed correctly everywhere.

    See https://github.com/enzoampil/fastquant/issues/382
    """

    file_description, path = tempfile.mkstemp(suffix=".png")
    fig.savefig(path, dpi=dpi)
    return Image(filename=path)

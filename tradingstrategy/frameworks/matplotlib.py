import tempfile

from IPython.core.display import Image
from matplotlib.figure import Figure


def render_figure_in_docs(fig: Figure, dpi=450):
    """Work around various matplotlib rendering issues to get inline images displayed correctly in notebooks everywhere.

    We have issues to render Backtrader figures in: PyCharm, Google Colab, etc.

    See https://github.com/enzoampil/fastquant/issues/382
    """

    # TODO: Do in-memory render instead
    file_description, path = tempfile.mkstemp(suffix=".png")

    # https://stackoverflow.com/questions/27878217/how-do-i-extend-the-margin-at-the-bottom-of-a-figure-in-matplotlib
    # https://stackoverflow.com/a/45632225/315168
    fig.savefig(path, dpi=dpi, bbox_inches='tight')
    return Image(filename=path)

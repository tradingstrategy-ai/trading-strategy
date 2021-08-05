from fastquant.backtest import post_backtest
from fastquant.backtest.post_backtest import print_dict


def _get_optim_metrics_and_params(sorted_metrics_df, sorted_params_df, verbose):
    # Save optimal parameters as dictionary
    optim_params = sorted_params_df.iloc[0].to_dict()

    sorted_metrics_df["max"] = str(sorted_metrics_df["max"])
    try:
        optim_metrics = sorted_metrics_df.iloc[0].to_dict()
        #
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/fastquant/backtest/post_backtest.py:246: in sort_metrics_params_and_strats
        #     optim_params = get_optim_metrics_and_params(
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/fastquant/backtest/post_backtest.py:266: in get_optim_metrics_and_params
        #     optim_metrics = sorted_metrics_df.iloc[0].to_dict()
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/pandas/core/series.py:1719: in to_dict
        #     return into_c((k, maybe_box_native(v)) for k, v in self.items())
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/pandas/core/series.py:1719: in <genexpr>
        #     return into_c((k, maybe_box_native(v)) for k, v in self.items())
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/pandas/core/dtypes/cast.py:185: in maybe_box_native
        #     if is_datetime_or_timedelta_dtype(value):
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/pandas/core/dtypes/common.py:1031: in is_datetime_or_timedelta_dtype
        #     return _is_dtype_type(arr_or_dtype, classes(np.datetime64, np.timedelta64))
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/pandas/core/dtypes/common.py:1604: in _is_dtype_type
        #     if hasattr(arr_or_dtype, "dtype"):
        # ../../../.cache/pypoetry/virtualenvs/capitalgram-1cXRS5Ur-py3.9/lib/python3.9/site-packages/backtrader/utils/autodict.py:104: in __getattr__
        #     return self[key]
        # _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
        #
        # self = AutoOrderedDict([('len', 110), ('drawdown', 72.14726300588684), ('moneydown', 141354.17673492432)])
        # key = 'dtype'
        #
        #     def __missing__(self, key):
        #         if self._closed:
        # >           raise KeyError
        # E           KeyError

    except KeyError:
        optim_metrics = {}

    if verbose > 0:
        print_dict(optim_params, "Optimal parameters:")
        print_dict(optim_metrics, "Optimal metrics:")

    return optim_params


def apply_patch():
    """Fix god known what issue with Backtrader autodict.

    https://github.com/miohtama/capitalgram-onchain-dex-quant-data/runs/3208273324?check_suite_focus=true
    """
    post_backtest.get_optim_metrics_and_params = _get_optim_metrics_and_params
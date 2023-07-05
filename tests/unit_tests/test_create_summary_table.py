import datetime
from tradingstrategy.utils.summarydataframe import (
    create_summary_table,
    as_dollar,
    as_duration,
    as_percent,
    as_integer
)


def test_create_summary_table_single_column():
    data = {
        "Annualised return %": as_percent(0.1),
        "Lifetime return %": as_percent(0.3),
        "Realised PnL": as_dollar(320),
        "Trade period": as_duration(datetime.timedelta(days=5, hours=2, minutes=3)),
    }

    df = create_summary_table(data, "", "Returns")

    assert df.shape == (4, 1)


def test_create_summary_table_multiple_columns():
    
    data3 = {
        "Number of positions": [
            as_integer(3),
            as_integer(5),
            as_integer(8),
        ],
        "% of total": [
            as_percent(0.375),
            as_percent(0.625),
            as_percent(1),
        ],
        "Average PnL %": [
            as_percent(0.06),
            as_percent(-0.02),
            as_percent(0.03),
        ],
    }

    df3 = create_summary_table(
        data3, ["Winning", "Losing", "Total"], "Closed Positions"
    )

    assert df3.shape == (3, 3)

import datetime
import pandas as pd
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

    data = ["10.00%", "30.00%", "$320.00", "5 days 2 hours"]
    index = ["Annualised return %", "Lifetime return %", "Realised PnL", "Trade period"]
    manual_df = pd.DataFrame(data, index=index)
    manual_df.index.name="Returns"
    manual_df.columns = [""]
    assert df.equals(manual_df)


def test_create_summary_table_multiple_columns():
    
    data = {
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

    df = create_summary_table(
        data, ["Winning", "Losing", "Total"], "Closed Positions"
    )

    assert df.shape == (3, 3)

    data = [["3", "5", "8"], ["37.50%", "62.50%", "100.00%"], ["6.00%", "-2.00%", "3.00%"]]
    index = ["Number of positions", "% of total", "Average PnL %"]
    manual_df = pd.DataFrame(data, index=index, columns=["Winning", "Losing", "Total"])
    manual_df.index.name="Closed Positions"
    assert df.equals(manual_df)

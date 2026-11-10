import datetime
import unittest

from tradingstrategy.utils.time import generate_monthly_timestamps


class MonthlyTimestampTest(unittest.TestCase):
    def test_end_on_monthly_boundary_is_not_repeated(self):
        start = datetime.datetime(2026, 1, 15)
        end = datetime.datetime(2026, 3, 15)

        self.assertEqual(
            generate_monthly_timestamps(start, end),
            [
                int(date.timestamp())
                for date in (start, datetime.datetime(2026, 2, 15), end)
            ],
        )

    def test_start_equals_end_is_not_repeated(self):
        date = datetime.datetime(2026, 1, 15)

        self.assertEqual(
            generate_monthly_timestamps(date, date), [int(date.timestamp())]
        )

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import core.dbmanagement as dbmanagement


def timestamp_days_ago(days):
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


class FakeCursor:
    def __init__(self, kkt_times, station_count):
        self.kkt_times = kkt_times
        self.station_count = station_count
        self.query = ""

    def execute(self, query):
        self.query = query

    def fetchall(self):
        if "pos_fiscals" in self.query:
            return self.kkt_times
        raise AssertionError(f"Unexpected fetchall query: {self.query}")

    def fetchone(self):
        if "pos_not_fiscals" in self.query:
            return (self.station_count,)
        raise AssertionError(f"Unexpected fetchone query: {self.query}")


class FakeDatabaseContext:
    def __init__(self, kkt_times, station_count):
        self.cursor = FakeCursor(kkt_times, station_count)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class DashboardStatsTest(unittest.TestCase):
    def make_queries(self, threshold):
        queries = dbmanagement.DbQueries.__new__(dbmanagement.DbQueries)
        queries.dont_valid_fn = threshold
        queries.get_default_dates = lambda: ("2026-08-19", "2026-09-30")
        queries.get_expire_fn = lambda start, end, show_marked: [{}, {}, {}]
        return queries

    def get_stats(self, queries, rows, station_count=5):
        fake_db = FakeDatabaseContext(rows, station_count)
        with patch.object(dbmanagement, "DatabaseContextManager", return_value=fake_db):
            return queries.get_dashboard_stats()

    def test_effective_time_fallback_and_missing_dates(self):
        fresh = timestamp_days_ago(1)
        expired = timestamp_days_ago(22)
        rows = [
            (expired, fresh),   # fresh v_time wins over old current_time
            (fresh, None),      # NULL v_time falls back to current_time
            (fresh, ""),        # empty v_time falls back to current_time
            (fresh, "None"),    # string None falls back to current_time
            (expired, None),    # old current_time without v_time is expired
            (fresh, expired),   # old v_time wins and is expired
            (None, None),       # no timestamp is not classified as expired
        ]

        stats = self.get_stats(self.make_queries(21), rows)

        self.assertEqual(stats["total_kkt"], 7)
        self.assertEqual(stats["active_kkt"], 4)
        self.assertEqual(stats["expired_kkt"], 2)
        self.assertEqual(stats["expire_fn"], 3)
        self.assertEqual(stats["all_stations"], 5)
        self.assertEqual(stats["day_filter_expire"], 21)

    def test_changed_threshold_is_used_for_label_and_counts(self):
        rows = [
            (timestamp_days_ago(29), None),
            (timestamp_days_ago(31), None),
        ]

        stats = self.get_stats(self.make_queries(30), rows)

        self.assertEqual(stats["active_kkt"], 1)
        self.assertEqual(stats["expired_kkt"], 1)
        self.assertEqual(stats["day_filter_expire"], 30)

    def test_database_failure_returns_safe_defaults(self):
        queries = self.make_queries(21)

        with patch.object(dbmanagement, "DatabaseContextManager", side_effect=RuntimeError("offline")), \
                patch.object(dbmanagement.core.logger.db_service, "error"):
            stats = queries.get_dashboard_stats()

        self.assertEqual(stats, {
            "total_kkt": 0,
            "active_kkt": 0,
            "expired_kkt": 0,
            "expire_fn": 0,
            "all_stations": 0,
            "day_filter_expire": 21,
        })


if __name__ == "__main__":
    unittest.main()

import base64
import configparser
import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import core.dbmanagement as dbmanagement
import posrelayd_db
from flask import Flask


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

    def get_filtered_data(self, queries, rows):
        columns = ["serialNumber", "current_time", "v_time", "licenses"]
        with patch.object(queries, "_get_pos_fiscals_rows", return_value=(rows, columns)):
            all_data, all_columns = queries.get_data_pos_fiscals()
            active_data, active_columns = queries.get_active_kkt()
            expired_data, expired_columns = queries.get_expired_kkt()

        self.assertEqual(all_columns, columns)
        self.assertEqual(active_columns, columns)
        self.assertEqual(expired_columns, columns)
        return all_data, active_data, expired_data, columns

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

    def test_filtered_tables_use_the_same_effective_time_rules_as_stats(self):
        fresh = timestamp_days_ago(1)
        expired = timestamp_days_ago(22)
        rows = [
            ("fresh-v-time", expired, fresh, None),
            ("null-v-time", fresh, None, None),
            ("empty-v-time", fresh, "", None),
            ("string-none-v-time", fresh, "None", None),
            ("old-v-time", fresh, expired, None),
            ("old-current-time", expired, None, None),
            ("no-dates", None, None, None),
        ]
        queries = self.make_queries(21)

        all_data, active_data, expired_data, columns = self.get_filtered_data(queries, rows)
        stats = self.get_stats(queries, [(row[1], row[2]) for row in rows])

        self.assertEqual([row[0] for row in active_data], [
            "fresh-v-time", "null-v-time", "empty-v-time", "string-none-v-time"])
        self.assertEqual([row[0] for row in expired_data], [
            "old-v-time", "old-current-time"])
        self.assertNotIn("no-dates", [row[0] for row in active_data + expired_data])
        self.assertEqual(len(active_data), stats["active_kkt"])
        self.assertEqual(len(expired_data), stats["expired_kkt"])
        self.assertEqual(len(all_data), stats["total_kkt"])
        self.assertTrue(all(len(row) == len(columns) + 1 for row in all_data))
        self.assertTrue(all(row[-1] is False for row in active_data))
        self.assertTrue(all(row[-1] is True for row in expired_data))

    def test_changed_threshold_is_used_for_label_and_counts(self):
        rows = [
            (timestamp_days_ago(29), None),
            (timestamp_days_ago(31), None),
        ]

        stats = self.get_stats(self.make_queries(30), rows)

        self.assertEqual(stats["active_kkt"], 1)
        self.assertEqual(stats["expired_kkt"], 1)
        self.assertEqual(stats["day_filter_expire"], 30)

    def test_changed_threshold_recalculates_filtered_tables_and_counts(self):
        rows = [
            ("within-30-days", timestamp_days_ago(29), None, None),
            ("over-30-days", timestamp_days_ago(31), None, None),
        ]
        queries = self.make_queries(30)

        _, active_data, expired_data, _ = self.get_filtered_data(queries, rows)
        stats = self.get_stats(queries, [(row[1], row[2]) for row in rows])

        self.assertEqual([row[0] for row in active_data], ["within-30-days"])
        self.assertEqual([row[0] for row in expired_data], ["over-30-days"])
        self.assertEqual(len(active_data), stats["active_kkt"])
        self.assertEqual(len(expired_data), stats["expired_kkt"])
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


class DashboardRoutesTest(unittest.TestCase):
    def setUp(self):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.server = posrelayd_db.WebServerRoute.__new__(posrelayd_db.WebServerRoute)
        self.server.app = Flask(
            "dashboard-routes-test",
            template_folder=os.path.join(project_root, "templates"),
            static_folder=os.path.join(project_root, "static"),
        )
        self.server.config = configparser.ConfigParser()
        self.server.config["webserver"] = {
            "user": "user",
            "pass": "1234",
            "admin": "admin",
            "admin_pass": "4321",
        }
        self.server.register_routes()
        self.client = self.server.app.test_client()

    @staticmethod
    def auth_headers():
        credentials = base64.b64encode(b"user:1234").decode("ascii")
        return {"Authorization": f"Basic {credentials}"}

    def test_filtered_routes_are_protected_like_fiscals(self):
        for path in ("/fiscals", "/active-kkt", "/expired-kkt"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 401)
                self.assertIn("Basic", response.headers["WWW-Authenticate"])

    def test_filtered_routes_reuse_fiscals_template_context(self):
        active_result = ([["active", False]], ["serialNumber"])
        expired_result = ([["expired", True]], ["serialNumber"])

        with patch.object(posrelayd_db.db_queries, "get_active_kkt", return_value=active_result), \
                patch.object(posrelayd_db, "render_template", return_value="rendered") as render:
            response = self.client.get("/active-kkt", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        render.assert_called_once_with(
            "fiscals.html",
            data=active_result[0],
            columns=active_result[1],
            default_visible_columns=self.server.default_visible_columns,
            enumerate=enumerate,
        )

        with patch.object(posrelayd_db.db_queries, "get_expired_kkt", return_value=expired_result), \
                patch.object(posrelayd_db, "render_template", return_value="rendered") as render:
            response = self.client.get("/expired-kkt", headers=self.auth_headers())

        self.assertEqual(response.status_code, 200)
        render.assert_called_once_with(
            "fiscals.html",
            data=expired_result[0],
            columns=expired_result[1],
            default_visible_columns=self.server.default_visible_columns,
            enumerate=enumerate,
        )

    def test_dashboard_links_target_filtered_routes(self):
        stats = {
            "total_kkt": 7,
            "active_kkt": 4,
            "expired_kkt": 2,
            "expire_fn": 3,
            "all_stations": 5,
            "day_filter_expire": 21,
        }
        with patch.object(posrelayd_db.db_queries, "get_dashboard_stats", return_value=stats):
            response = self.client.get("/", headers=self.auth_headers())

        page = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertRegex(page, r'href="/fiscals"[\s\S]+?Всего ККТ')
        self.assertRegex(page, r'href="/active-kkt"[\s\S]+?Активные ККТ')
        self.assertEqual(page.count('href="/expired-kkt"'), 2)
        self.assertIn('href="/expire_fn"', page)
        self.assertIn('href="/onlypos"', page)

    def test_manual_date_filters_keep_their_original_fields(self):
        with patch.object(posrelayd_db.db_queries, "search_dont_update", return_value=([], [])) as search, \
                patch.object(posrelayd_db, "render_template", return_value="rendered"):
            response = self.client.post(
                "/dont-update", data={"search_query": "21"}, headers=self.auth_headers())
            self.assertEqual(response.status_code, 200)
            search.assert_called_once_with("current_time", 21)

        with patch.object(posrelayd_db.db_queries, "search_dont_update", return_value=([], [])) as search, \
                patch.object(posrelayd_db, "render_template", return_value="rendered"):
            response = self.client.post(
                "/dont-validation", data={"search_query": "30"}, headers=self.auth_headers())
            self.assertEqual(response.status_code, 200)
            search.assert_called_once_with("v_time", 30)


if __name__ == "__main__":
    unittest.main()

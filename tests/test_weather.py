import os
import sys
import time
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# shared_state exige estas variables al importarse; valores de mentira para las pruebas.
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("TELEGRAM_CHAT_ID", "0")

import weather  # noqa: E402

HAVANA = ZoneInfo("America/Havana")

SAMPLE = {
    "current": {"time": "2026-11-16T08:00", "cloud_cover": 3, "weather_code": 0, "is_day": 1},
    "daily": {"sunrise": ["2026-11-16T06:43"], "sunset": ["2026-11-16T17:44"]},
}


def _reset():
    weather._cache = None
    weather._refreshing = False
    weather._next_try = 0.0
    weather.ENABLED = True


class ParseTests(unittest.TestCase):
    def test_maps_cloud_code_day_and_sun_times_to_timestamps(self):
        sky = weather.parse_open_meteo(SAMPLE, HAVANA)
        self.assertEqual(sky["cloud_pct"], 3)
        self.assertEqual(sky["code"], 0)
        self.assertIs(sky["is_day"], True)
        self.assertEqual(sky["sunrise_ts"], datetime(2026, 11, 16, 6, 43, tzinfo=HAVANA).timestamp())
        self.assertEqual(sky["sunset_ts"], datetime(2026, 11, 16, 17, 44, tzinfo=HAVANA).timestamp())

    def test_clamps_cloud_cover_to_0_100(self):
        data = {**SAMPLE, "current": {**SAMPLE["current"], "cloud_cover": 140}}
        self.assertEqual(weather.parse_open_meteo(data, HAVANA)["cloud_pct"], 100)

    def test_malformed_payload_raises_value_error(self):
        for bad in ({}, {"current": {}}, {"current": SAMPLE["current"], "daily": {}}):
            with self.assertRaises(ValueError):
                weather.parse_open_meteo(bad, HAVANA)


class CacheTests(unittest.TestCase):
    def setUp(self):
        _reset()
        self._orig_fetch = weather._fetch
        self._orig_spawn = weather._spawn
        self.spawned = 0

        def fake_spawn():
            self.spawned += 1

        weather._spawn = fake_spawn

    def tearDown(self):
        weather._fetch = self._orig_fetch
        weather._spawn = self._orig_spawn
        _reset()

    def test_refresh_success_stores_sky_with_updated_ts(self):
        weather._fetch = lambda: weather.parse_open_meteo(SAMPLE, HAVANA)
        before = time.time()
        weather.refresh()
        sky = weather.get_sky()
        self.assertEqual(sky["cloud_pct"], 3)
        self.assertGreaterEqual(sky["updated_ts"], before)
        self.assertEqual(self.spawned, 0, "fresh cache must not trigger another refresh")

    def test_refresh_failure_keeps_last_good_value_and_does_not_raise(self):
        weather._fetch = lambda: weather.parse_open_meteo(SAMPLE, HAVANA)
        weather.refresh()

        def boom():
            raise RuntimeError("network down")

        weather._fetch = boom
        weather.refresh()
        self.assertEqual(weather.get_sky()["cloud_pct"], 3)

    def test_get_sky_without_data_returns_none_and_starts_one_refresh(self):
        self.assertIsNone(weather.get_sky())
        self.assertEqual(self.spawned, 1)

    def test_stale_cache_is_served_and_refreshed_in_background(self):
        weather._fetch = lambda: weather.parse_open_meteo(SAMPLE, HAVANA)
        weather.refresh()
        weather._cache["updated_ts"] -= weather.REFRESH_SECONDS + 5
        sky = weather.get_sky()
        self.assertEqual(sky["cloud_pct"], 3)
        self.assertEqual(self.spawned, 1)

    def test_failed_refresh_backs_off_before_retrying(self):
        def boom():
            raise RuntimeError("network down")

        weather._fetch = boom
        weather.refresh()
        self.assertIsNone(weather.get_sky())
        self.assertEqual(self.spawned, 0, "must not retry inside the backoff window")

    def test_disabled_returns_none_and_never_fetches(self):
        weather.ENABLED = False
        self.assertIsNone(weather.get_sky())
        self.assertEqual(self.spawned, 0)


if __name__ == "__main__":
    unittest.main()

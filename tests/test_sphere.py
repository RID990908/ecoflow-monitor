import os
import sys
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

if sys.version_info < (3, 10):  # dashboard_server usa `dict | None`
    raise unittest.SkipTest("requiere Python 3.10+")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("TELEGRAM_CHAT_ID", "0")

import dashboard_server  # noqa: E402

HAVANA = ZoneInfo("America/Havana")
CAP = 2048.0  # Delta 2 + batería extra
NOW = datetime(2026, 11, 16, 21, 25, tzinfo=HAVANA)  # faltan 10 h 35 min para las 08:00


def sphere(soc, in_w, out_w, now=NOW, reserve=15, night_load=None):
    return dashboard_server._sphere_state(now, soc, in_w, out_w, CAP, reserve, night_load)


class SphereStateTests(unittest.TestCase):
    def test_charging_when_input_exceeds_output(self):
        s = sphere(62, 850, 120)
        self.assertEqual(s["state"], "charging")
        self.assertIsNone(s["dawn_pct"])
        self.assertEqual(s["reserve_pct"], 15)

    def test_no_input_and_battery_will_not_reach_dawn_is_at_risk(self):
        # 62% - 120 W * 10.58 h / 2048 Wh = 62% - 62% -> 0% < 15%
        s = sphere(62, 0, 120)
        self.assertEqual(s["state"], "at_risk")
        self.assertLess(s["dawn_pct"], 15)
        self.assertGreaterEqual(s["dawn_pct"], 0)

    def test_no_input_and_battery_reaches_dawn_with_reserve_is_ok(self):
        # 85% - 62% = 23% >= 15%
        s = sphere(85, 0, 120)
        self.assertEqual(s["state"], "ok")
        self.assertAlmostEqual(s["dawn_pct"], 23.0, delta=1.0)

    def test_input_equal_to_output_is_not_charging(self):
        self.assertEqual(sphere(62, 120, 120)["state"], "at_risk")

    def test_uses_expected_night_load_over_instant_output(self):
        # salida instantánea alta, pero el consumo nocturno esperado es bajo -> sí llega
        self.assertEqual(sphere(62, 0, 600, night_load=60)["state"], "ok")

    def test_before_dawn_counts_hours_to_the_same_day_08(self):
        early = datetime(2026, 11, 16, 5, 0, tzinfo=HAVANA)  # faltan 3 h
        self.assertEqual(sphere(40, 0, 120, now=early)["state"], "ok")

    def test_missing_data_returns_none(self):
        self.assertIsNone(sphere(None, 0, 120))
        self.assertIsNone(sphere(62, 0, None))

    def test_dawn_pct_never_negative(self):
        self.assertEqual(sphere(10, 0, 500)["dawn_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()

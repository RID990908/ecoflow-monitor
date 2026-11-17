import os
import sys
import unittest

if sys.version_info < (3, 10):  # dashboard_server usa `dict | None`
    raise unittest.SkipTest("requiere Python 3.10+")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("TELEGRAM_CHAT_ID", "0")

import dashboard_server  # noqa: E402


class DischargeLimitTests(unittest.TestCase):
    def test_reads_the_limit_set_on_the_device(self):
        self.assertEqual(dashboard_server.get_min_discharge_soc({"bms_emsStatus.minDsgSoc": 15}), 15)

    def test_zero_means_limit_removed_and_is_kept(self):
        self.assertEqual(dashboard_server.get_min_discharge_soc({"bms_emsStatus.minDsgSoc": 0}), 0)

    def test_missing_field_is_none(self):
        self.assertIsNone(dashboard_server.get_min_discharge_soc({"bms_bmsStatus.soc": 80}))


if __name__ == "__main__":
    unittest.main()

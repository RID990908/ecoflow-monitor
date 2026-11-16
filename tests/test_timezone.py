import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# shared_state exige estas variables al importarse; valores de mentira para las pruebas.
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test")
os.environ.setdefault("TELEGRAM_CHAT_ID", "0")
# El valor por defecto es lo que se prueba: sin TZ_NAME definida.
os.environ.pop("TZ_NAME", None)

import shared_state  # noqa: E402
import weather  # noqa: E402


class ServerTimezoneTests(unittest.TestCase):
    def test_default_server_zone_is_fixed_utc_minus_4(self):
        # Cuba no volvió a la hora estándar: los relojes reales van en UTC-4 todo el año.
        for month in (1, 7, 11):
            when = datetime(2026, month, 16, 12, 0, tzinfo=shared_state.TZ)
            self.assertEqual(when.utcoffset(), timedelta(hours=-4), f"mes {month}")

    def test_open_meteo_local_times_keep_the_same_physical_sun_instants(self):
        # Open-Meteo devuelve la hora local en la zona pedida (str(TZ)); con una zona
        # fija coincide con la del servidor, así que amanecer/atardecer no se mueven.
        sample = {
            "current": {"time": "2026-11-16T12:00", "cloud_cover": 3, "weather_code": 0, "is_day": 1},
            "daily": {"sunrise": ["2026-11-16T07:43"], "sunset": ["2026-11-16T18:44"]},
        }
        sky = weather.parse_open_meteo(sample, shared_state.TZ)
        self.assertEqual(sky["sunrise_ts"], datetime(2026, 11, 16, 11, 43, tzinfo=timezone.utc).timestamp())
        self.assertEqual(sky["sunset_ts"], datetime(2026, 11, 16, 22, 44, tzinfo=timezone.utc).timestamp())

    def test_weather_asks_open_meteo_for_the_same_zone_as_the_server(self):
        self.assertEqual(str(shared_state.TZ), "Etc/GMT+4")


if __name__ == "__main__":
    unittest.main()

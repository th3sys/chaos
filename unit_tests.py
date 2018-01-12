import unittest
import contracts as cont
import datetime
from dateutil.relativedelta import relativedelta


class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_filter(self):
        positions = {
            "positions": [{
                "position": {
                    "contractSize": 1.0,
                    "createdDate": "2018/01/12 08:44:15:000",
                    "createdDateUTC": "2018-01-12T08:44:15",
                    "dealId": "DIAAAABPCZSKTAX",
                    "dealReference": "GHADVYJU66YL4TP",
                    "size": 100.0,
                    "direction": "SELL",
                    "limitLevel": None,
                    "level": 10.38,
                    "currency": "GBP",
                    "controlledRisk": False,
                    "stopLevel": None,
                    "trailingStep": None,
                    "trailingStopDistance": None,
                    "limitedRiskPremium": None
                },
                "market": {
                    "instrumentName": "Volatility Index",
                    "expiry": "JAN-18",
                    "epic": "IN.D.VIX.MONTH2.IP",
                    "instrumentType": "INDICES",
                    "lotSize": 1.0,
                    "high": 10.63,
                    "low": 10.38,
                    "percentageChange": -0.67,
                    "netChange": -0.07,
                    "bid": None,
                    "offer": None,
                    "updateTime": "08:43:41",
                    "updateTimeUTC": "08:43:41",
                    "delayTime": 0,
                    "streamingPricesAvailable": True,
                    "marketStatus": "TRADEABLE",
                    "scalingFactor": 1
                }
            }]
        }
        pos = [p['position'] for p in positions['positions'] if p['position']['dealReference'] == 'GHADVYJU66YL4TP']
        self.assertEqual(pos[0]['level'], 10.38)

    def test_join(self):
        pending = [(1, 'VX', 100), (2, 'VX', 400), (3, 'DAX', 100)]
        known = [('VX', 200), ('ES', 300)]
        valid = [kOrder + pOrder for kOrder in known for pOrder in pending if kOrder[0] == pOrder[1]]
        self.assertTrue(len(valid) == 2)
        self.assertEqual(valid[0], ('VX', 200, 1, 'VX', 100))
        self.assertEqual(valid[1], ('VX', 200, 2, 'VX', 400))

    def test_one_day_before(self):
        today = datetime.date(2017, 11, 14)
        sec = cont.SecurityDefinition()
        expiry = sec.get_vix_expiry_date(today)
        print(expiry - relativedelta(days=+1))
        self.assertEqual(today, expiry - relativedelta(days=+1))

    def test_on_the_day(self):
        today = datetime.date(2017, 11, 15)
        sec = cont.SecurityDefinition()
        expiry = sec.get_vix_expiry_date(today)
        print(expiry - relativedelta(days=+1))
        self.assertGreater(today, expiry - relativedelta(days=+1))

    def test_one_day_after(self):
        today = datetime.date(2017, 11, 16)
        sec = cont.SecurityDefinition()
        expiry = sec.get_vix_expiry_date(today)
        print(expiry - relativedelta(days=+1))
        self.assertGreater(today, expiry - relativedelta(days=+1))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

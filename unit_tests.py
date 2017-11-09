import unittest
import contracts as cont
import datetime
from dateutil.relativedelta import relativedelta


class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

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

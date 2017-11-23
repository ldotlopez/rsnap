import unittest
from datetime import datetime


import rsnap


class ProfilesTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(year=2017, month=11, day=20, hour=18, minute=49, second=12)

    def get_ids(self, cls):
        g = cls(basedir='/tmp/foo', now=self.now)
        return list(g.backcounter())

    def test_monthly(self):
        ids = self.get_ids(rsnap.MonthlyProfile)
        self.assertEqual(
            ids,
            ['11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01', '12']
        )

    def test_weekly(self):
        ids = self.get_ids(rsnap.WeeklyProfile)

        self.assertEqual(
            len(ids),
            53)

        # Yes, 47 is in both ends, weeks doesn't align perfectly with years
        self.assertEqual(
            ids[:2] + ids[-2:],
            ['47', '46', '48', '47'])

    def test_subdaily(self):
        ids = self.get_ids(rsnap.SubdailyProfile)

        self.assertEqual(
            len(ids),
            24*12)

        self.assertEqual(
            ids[:2] + ids[-2:],
            ['18.45.00', '18.40.00', '18.55.00', '18.50.00'])

    def test_workday(self):
        ids = self.get_ids(rsnap.WorkdayProfile)

        self.assertEqual(
            len(ids),
            7)

        self.assertEqual(
            ids,
            ['1', '7', '6', '5', '4', '3', '2'])


if __name__ == '__main__':
    unittest.main()

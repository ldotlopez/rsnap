import unittest
import StringIO
from datetime import datetime

import rsnap


def clean_config(s):
    return '\n'.join([x.strip() for x in s.strip().split('\n')])


class ProfilesTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(year=2017, month=11, day=20,
                            hour=18, minute=49, second=12)

    def get_ids(self, cls):
        g = cls(basedir='/tmp/foo', now=self.now)
        return list(g.backcounter())

    def test_subdaily(self):
        ids = self.get_ids(rsnap.SubdailyProfile)

        self.assertEqual(
            len(ids),
            24*12)

        self.assertEqual(
            ids[:2] + ids[-2:],
            ['18.45.00', '18.40.00', '18.55.00', '18.50.00'])

    def test_monthly(self):
        ids = self.get_ids(rsnap.MonthlyProfile)
        self.assertEqual(
            ids,
            ['11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01',
             '12']
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

    def test_monthday(self):
        ids = self.get_ids(rsnap.MonthdayProfile)

        self.assertEqual(
            len(ids),
            31)

        self.assertEqual(
            ids[0:3],
            ['21', '22', '23'])

        self.assertEqual(
            ids[-3:],
            ['18', '19', '20'])

    def test_weekday(self):
        ids = self.get_ids(rsnap.WeekdayProfile)

        self.assertEqual(
            len(ids),
            7)

        self.assertEqual(
            ids,
            ['1', '7', '6', '5', '4', '3', '2'])


if __name__ == '__main__':
    unittest.main()

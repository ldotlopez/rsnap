import unittest
import StringIO
import sys
from datetime import datetime

import rsnap


def clean_config(s):
    return '\n'.join([x.strip() for x in s.strip().split('\n')])


class SequenceProfile(rsnap.CyclicProfile):
    NAME = 'sequence'

    def __init__(self, seq=None, *args, **kwargs):
        if seq is None:
            seq = range(9, -1, -1)

        self.seq = seq
        super(SequenceProfile, self).__init__(*args, **kwargs)

    def backcounter(self):
        for x in self.seq:
            yield str(x)


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
            ids[:3] + ids[-3:],
            ['47', '46', '45', '49', '48', '47'])

    def test_monthday(self):
        ids = self.get_ids(rsnap.MonthdayProfile)
        self.assertEqual(
            len(ids),
            31)

        self.assertEqual(
            ids[:3],
            ['20', '19', '18'])

        self.assertEqual(
            ids[-3:],
            ['23', '22', '21'])

    def test_weekday(self):
        ids = self.get_ids(rsnap.WeekdayProfile)

        self.assertEqual(
            len(ids),
            7)

        self.assertEqual(
            ids,
            ['1', '7', '6', '5', '4', '3', '2'])


class RSnapTest(unittest.TestCase):
    def test_simple(self):
        rs = rsnap.RSnap('/foo', storage='/storage', profile='sequence')
        (rsync, src, dst), kwargs = rs.build()

        self.assertEqual(kwargs.get('link-dest'), None)

        self.assertEqual(src, '/foo')
        self.assertEqual(dst, '/storage/sequence/9')

    def test_previous(self):
        rs = rsnap.RSnap('/foo', storage='/storage', profile='sequence')

    def test_rsync_path_and_opts(self):
        rs = rsnap.RSnap('/foo', storage='/storage', profile='sequence',
                         rsync_bin='/opt/rsync', rsync_opts={
                            'opt-bool': True,
                            'opt-str': 'abc'
                         })
        (rsync, src, dst), kwargs = rs.build()

        self.assertEqual(rsync, '/opt/rsync')
        self.assertEqual(kwargs.get('opt-bool', None), True)
        self.assertEqual(kwargs.get('opt-str', None), 'abc')


if __name__ == '__main__':
    unittest.main()

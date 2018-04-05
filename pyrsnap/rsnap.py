#!/usr/bin/env python2

from __future__ import print_function

import argparse
import collections
import ConfigParser
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta


class ExecutionError(Exception):
    pass


class StorageProfile(object):
    NAME = 'default'

    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            for x in subclass.get_subclasses():
                yield x

            yield subclass

    @classmethod
    def get_subclass(cls, name):
        for cls in cls.get_subclasses():
            if cls.NAME == name:
                return cls

        raise TypeError(name)

    def __init__(self, basedir):
        self._basedir = basedir

    @property
    def basedir(self):
        return self._basedir + '/' + self.NAME

    def get_current_storage(self):
        now = time.time()
        dt = datetime.fromtimestamp(now)
        fmt = dt.strftime('%Y.%m.%d-%H.%M.%S.%f')
        return self.basedir + '/' + fmt

    def get_previous_storage(self):
        try:
            return self.basedir + '/' + sorted(os.listdir(self.basedir))[-1]
        except (IndexError, OSError):
            return None


class CyclicProfile(StorageProfile):
    def __init__(self, *args, **kwargs):
        now = kwargs.pop('now', datetime.now())
        super(CyclicProfile, self).__init__(*args, **kwargs)

        self.now = now
        self.g = self.backcounter()
        self.curr_id = next(self.g)

    def get_current_storage(self):
        return os.path.realpath(self.basedir + '/' + self.curr_id)

    def get_previous_storage(self):
        for x in self.g:
            if x == self.curr_id:
                return None

            test = self.basedir + '/' + x
            if os.path.exists(test):
                return os.path.realpath(test)

    def backcounter(self):
        raise StopIteration()


class SubdailyProfile(CyclicProfile):
    """
    288 copies by default (one each 5 minutes)
    """
    NAME = 'subdaily'

    def __init__(self, *args, **kwargs):
        self.interval = kwargs.pop('interval', 5*60)
        super(SubdailyProfile, self).__init__(*args, **kwargs)

    def backcounter(self):
        ts = time.mktime(self.now.timetuple())
        ts = self.interval * (ts // self.interval)

        now = datetime.fromtimestamp(ts)
        day = timedelta(days=1)
        diff = timedelta(seconds=0)

        while True:
            curr = now - diff
            yield '%02d.%02d.%02d' % (curr.hour, curr.minute, curr.second)

            diff = diff + timedelta(seconds=self.interval)
            if diff >= day:
                break


class MonthlyProfile(CyclicProfile):
    """
    12 copies
    """
    NAME = 'monthly'

    def backcounter(self):
        for m in range(self.now.month + 12, self.now.month, -1):
            m = m % 12 or 12
            yield '%02d' % m


class WeeklyProfile(CyclicProfile):
    """
    52 copies (or 53...)
    """
    NAME = 'weekly'

    def backcounter(self):
        year = timedelta(days=367)
        diff = timedelta(weeks=0)

        while True:
            week = (self.now - diff).isocalendar()[1]
            yield '%02d' % week

            diff = diff + timedelta(weeks=1)
            if diff >= year:
                break


class HourlyProfile(SubdailyProfile):
    """
    24 copies
    """
    NAME = 'hourly'

    def __init__(self, *args, **kwargs):
        kwargs['interval'] = 60*60
        super(HourlyProfile, self).__init__(*args, **kwargs)


class MonthdayProfile(CyclicProfile):
    """
    30-31 copies
    """
    NAME = 'monthday'

    def backcounter(self):
        if self.now.month == 1:
            prev = self.now.replace(year=self.now.year - 1, month=12)
        else:
            prev = self.now.replace(month=self.now.month - 1)

        while self.now > prev:
            prev = prev + timedelta(days=1)
            yield '%02d' % prev.day


class WeekdayProfile(CyclicProfile):
    """
    7 copies
    """
    NAME = 'weekday'

    def backcounter(self):
        for wd in range(self.now.isoweekday() + 7, self.now.isoweekday(), -1):
            wd = wd % 7 or 7
            yield '%d' % wd


class ArgumentSet(dict):
    def copy(self):
        return self.__class__(**self)

    def merge(self, *argsets):
        for argset in argsets:
            self.update(argset)

    def as_command_line(self):
        ret = []

        for (k, v) in self.items():
            # Drop this argument
            if v is None:
                continue

            # Command line arguments use '-'
            k = k.replace('_', '-')

            # Short options
            if len(k) == 1:
                if v is True:
                    # Add as -x
                    ret.append('-%s' % k)
                elif v is False:
                    # Skip negatives
                    continue
                else:
                    # Short options cannot have arguments
                    raise ValueError(self)

            # Long options
            else:
                if v is True:
                    ret.append('--%s' % k)
                elif v is False:
                    ret.append('--no-%s' % k)
                else:
                    ret.append('--%s=%s' % (k, v))

        return ret


class RSnap(object):
    RSYNC_BIN = '/usr/bin/rsync'
    RSYNC_OPTS = {
        'acls': True,
        'archive': True,
        'delete': True,
        'fake-super': True,
        'hard-links': True,
        'inplace': False,
        'numeric-ids': True,
        'one-file-system': True,
        'partial': True,
        'progress': True,
        'rsh': 'ssh -oPasswordAuthentication=no -oStrictHostKeyChecking=no',
        'rsync-path': 'rsync --fake-super',
        'verbose': False,
        'xattrs': True
    }

    def __init__(self, rsync_bin=None, rsync_opts=None):
        if rsync_bin is None:
            rsync_bin = self.RSYNC_BIN
        if rsync_opts is None:
            rsync_opts = self.RSYNC_OPTS

        self.rsync_bin = rsync_bin
        self.rsync_opts = ArgumentSet(**rsync_opts)

    def _get_base_command_line(self, opts=None):
        argset = self.rsync_opts.copy()
        if opts:
            argset.merge(opts)

        return [self.rsync_bin] + argset.as_command_line()

    def build(self, profile, storage, source, rsync_opts=None):
        try:
            is_class = issubclass(profile, StorageProfile)
        except TypeError:
            is_class = False

        if os.path.exists(storage + '/exclude.lst'):
            rsync_opts.update({
                'exclude-from': storage + '/exclude.lst',
                'delete-excluded': True
            })

        if not is_class:
            profile_cls = (StorageProfile.get_subclass(profile)
                           or StorageProfile)
            profile = profile_cls(basedir=storage)

        cmd = self._get_base_command_line(rsync_opts)
        link_dest = profile.get_previous_storage()

        if link_dest:
            cmd.append('--link-dest=%s' % (link_dest,),)

        dest = profile.get_current_storage()
        if source.endswith('/'):
            dest += '/'

        cmd.extend([
            '%s' % (source,),
            '%s' % (dest,)
        ])

        return cmd

    def run(self, profile, storage, source, rsync_opts=None):
        cmd = self.build(profile, storage, source, rsync_opts)
        print(' '.join(
            ['%s' % x for x in cmd]
        ))
        dest = cmd[-1]
        try:
            os.makedirs(dest)
        except OSError:
            pass

        try:
            res = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            if e.returncode not in (23,):  # whitelisted codes
                raise ExecutionError(returncode=e.returncode, output=e.output)

        latest = os.path.dirname(os.path.realpath(dest)) + "/latest"
        try:
            os.unlink(latest)
        except OSError as e:
            pass

        try:
            # Basenaming link source provides better isolation of backup
            os.symlink(os.path.basename(dest.rstrip('/')), latest)
        except OSError as e:
            print("Unable to link '%s' to '%s': %s" % (dest, latest, repr(e)))


def build_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--rsync-bin',
        default='/usr/bin/rsync',
        required=False)
    parser.add_argument(
        '-c', '--config',
        required=False)
    parser.add_argument(
        '--storage',
        required=False),
    parser.add_argument(
        '--profile',
        required=False)
    parser.add_argument(
        nargs='?',
        dest='source')

    return parser


def operations_from_config(fp):
    parser = ConfigParser.SafeConfigParser()
    parser.readfp(fp)

    return [
        (sect, {
            'source': parser.get(sect, 'source'),
            'storage': parser.get(sect, 'storage'),
            'profile': parser.get(sect, 'profile'),
        })
        for sect in parser.sections()]


def operations_from_args(args):
    return [('Command Line', {
        'storage': args['storage'],
        'source': args['source'],
        'profile': args['profile']
    })]


def main():
    parser = build_argparser()
    args = vars(parser.parse_args(sys.argv[1:]))

    # Check arguments
    if not args['config']:
        manual_reqs = ('profile', 'storage', 'source')
        for arg in manual_reqs:
            if not args.get(arg):
                parser.print_usage()
                errmsg = "'%(arg)s' is required without config"
                errmsg = errmsg % {'arg': arg}
                print(errmsg, file=sys.stderr)
                sys.exit(1)

    # Build operations
    if args['config']:
        with open(args['config']) as fp:
            operations = operations_from_config(fp)

    else:
        operations = operations_from_args(args)

    # Run operations
    for (name, items) in operations:
        try:
            RSnap().build(
                profile=items['profile'],
                storage=items['storage'],
                source=items['source'],
                rsync_opts={
                    'acls': False,
                    'progress': False,
                    'verbose': False
                }
            )

        except ExecutionError as e:
            errmsg = "Backup of %(src)s failed: %(code)s"
            errmsg = errmsg % {'src': src, 'code': e.returncode}
            print(errmsg, file=sys.stderr)

            for line in e.output.split('\n'):
                print("\t" + line)

            continue

        print("Backup of %(src)s: OK" % {'src': items['source']})


if __name__ == '__main__':
    main()

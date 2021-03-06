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
    def __init__(self, returncode, output):
        self.returncode = returncode
        self.output = output

    def repr(self):
        return "ExecutionError: (%(code)d, %(output)s" % {
            'code': self.returncode,
            'output': self.output
        }


class MissingPathError(Exception):
    pass


class StorageProfile(object):
    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            for x in subclass.get_subclasses():
                yield x

            yield subclass

    @classmethod
    def get_subclass(cls, name):
        for cls in cls.get_subclasses():
            if getattr(cls, 'NAME', None) == name:
                return cls

        raise TypeError(name)

    def __init__(self, basedir):
        self._basedir = basedir

    @property
    def basedir(self):
        return self._basedir + '/' + self.NAME

    def get_current_storage(self):
        raise NotImplementedError()

    def get_previous_storage(self):
        raise NotImplementedError()


class SnapshotProfile(StorageProfile):
    NAME = 'snapshot'

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

        counter = self.now
        while counter > prev:
            yield '%02d' % counter.day
            counter = counter - timedelta(days=1)


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
        'acls': False,
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

    def __init__(self, source, storage, profile='default',
                 rsync_bin=None, rsync_opts=None):
        self.rsync_bin = rsync_bin or self.RSYNC_BIN

        self.rsync_opts = self.RSYNC_OPTS
        self.rsync_opts.update(rsync_opts or {})
        self.rsync_opts = ArgumentSet(**self.rsync_opts)

        self.source = source
        self.storage = storage

        try:
            profile_is_class = issubclass(profile, StorageProfile)
        except TypeError:
            profile_is_class = False

        if not profile_is_class:
            profile_cls = (StorageProfile.get_subclass(profile)
                           or StorageProfile)
            self.profile = profile_cls(basedir=self.storage)
        else:
            self.profile = profile

    def _get_base_command_line(self, opts=None):
        argset = self.rsync_opts.copy()
        if opts:
            argset.merge(opts)

        return [self.rsync_bin] + argset.as_command_line()

    def _get_excludes(self):
        excludes_file = self.storage + '/exclude.lst'
        try:
            fh = open(excludes_file, 'r')
            fh.close()
            return excludes_file

        except IOError:
            raise MissingPathError(excludes_file)

    def _get_latest(self, destination):
        return (
            os.path.basename(destination.rstrip('/')),
            os.path.dirname(os.path.realpath(destination)) + "/latest"
        )

    def build(self):
        kwargs = ArgumentSet(**self.rsync_opts)

        try:
            # Not sure if one implies the other
            kwargs['exclude-from'] = self._get_excludes()
            kwargs['delete-excluded'] = True
        except MissingPathError:
            pass

        # If link_dest is None its unset, it's OK
        link_dest = self.profile.get_previous_storage()
        kwargs['link-dest'] = link_dest

        dest = self.profile.get_current_storage()
        if self.source.endswith('/'):
            dest += '/'

        return (self.rsync_bin, self.source, dest), kwargs

    def run(self):
        (cmd_bin, cmd_src, cmd_dst), cmd_kwargs = self.build()

        # FIXME: Use a logger
        cmd = (
            [cmd_bin] +
            cmd_kwargs.as_command_line() +
            [cmd_src, cmd_dst]
        )
        cmd_str = ["'" + x + "'" for x in cmd]
        cmd_str = ' '.join(cmd_str)
        print(cmd_str)

        # Try to create cmd_dst parents
        try:
            os.makedirs(cmd_dst)
        except OSError:
            pass

        # Execute
        try:
            res = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            if e.returncode not in (23,):  # whitelisted codes
                raise ExecutionError(returncode=e.returncode, output=e.output)

        # Update latest link
        (latest_src, latest_dst) = self._get_latest(cmd_dst)
        try:
            os.unlink(latest_dst)
        except OSError as e:
            pass

        try:
            # Basenaming link source provides better isolation of backup
            os.symlink(latest_src, latest_dst)
        except OSError as e:
            print("Unable to link '%s' to '%s': %s" % (
                latest_src, latest_dst, repr(e))
            )


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

    ret = []
    for sect in [sect for sect in parser.sections() if sect != 'DEFAULT']:
        values = {x: parser.get(sect, x)
                  for x in ('profile', 'source', 'storage')}

        try:
            values['rsync_bin'] = parser.get(sect, 'rsync-bin')
        except ConfigParser.NoOptionError:
            pass

        values['rsync_opts'] = {
            k[len('rsync-opt-'):]: v
            for (k, v) in parser.items(sect)
            if k.startswith('rsync-opt-')}

        transformations = {
            'True': True,
            'False': False,
            'None': None,
            '': None
        }
        values['rsync_opts'] = {
            (k, transformations.get(v, v))
            for (k, v) in values['rsync_opts'].items()
        }

        ret.append((sect, values))

    return ret


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
    for (name, item) in operations:
        try:
            rsnap = RSnap(
                source=item['source'],
                storage=item['storage'],
                profile=item['profile'],
                rsync_bin=item.get('rsync_bin', None),
                rsync_opts=item.get('rsync_opts', None))
            rsnap.run()

        except ExecutionError as e:
            errmsg = "Backup of %(src)s failed: %(code)s"
            errmsg = errmsg % {'src': item['source'], 'code': e.returncode}
            print(errmsg, file=sys.stderr)

            for line in e.output.split('\n'):
                print("\t" + line)

            continue

        print("Backup of %(name)s: OK" % {'name': name})


if __name__ == '__main__':
    main()

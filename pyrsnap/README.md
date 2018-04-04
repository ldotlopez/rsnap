
rsnap.py
===

Inspired by [rsnapshot](http://rsnapshot.org/)


Usage
---

rsnap.py has two modes: manual (explicit) or automatic (using a configuration
file).

In manual mode you have to specify three arguments:

  - `--profile`: Which profile of rotation to use.
  - `--storage`: Destination of backups. rsnap.py uses a special hierarchy to store backups
  - `source`: Location to backup. Can be a local path or remote location (rsync style)

In automatic mode you have to specify a configuration file using the
`--config` option. Current format is a direct translation of manual mode, here is an example:

```
[DEFAULT]
storage-root = /backups
profile = weekday

[host-a]
storage = %(storage-root)s/host-a
source = root@host-a:/

[host-b]
profile = hourly
storage = /another-location
source = root@host-b:/
```

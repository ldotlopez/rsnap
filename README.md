
rsnap.py
===

Inspired by [rsnapshot](http://rsnapshot.org/)


Usage
---

rsnap.py can operate in two modes: manual (explicit) or automatic (using a configuration file).

Manual mode requires three arguments:

  - `--profile`: Which profile of rotation to use.
  - `--storage`: Destination of backups. rsnap.py uses a special hierarchy to store backups.
  - `source`: Location to backup. Can be a local path or remote location (rsync style).

In the automatic mode a configuration file is required (using the `--config` option).
Current format is a direct translation of manual mode, here is an example:

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

Rotation profiles
---

  - hourly: 24 copies, one per hour (00.00.00, 01.00.00, …, 23.00.00)
  - weekday: 7 copies, from Monday to Sunday (1, 2, …, 6, 7)
  - weekly: 52-53 copies, one per week (01, 02, …, 51, 52)
  - monthday: 30-31 copies, from the first day of the month to the last (01, 02, …, 30, 31)
  - monthly: 12 copies, from January to December (01, 02, …, 11, 12)

#!/usr/bin/env python3
"""
This program formats output of open-source tool mpt-status as prometheus metrics

To test run
python -m doctest mpt-status.py
"""

import sys
import re
from subprocess import run, PIPE, CalledProcessError, TimeoutExpired
from collections import namedtuple

CMD_TIMEOUT = 30  # seconds
CMD_PROBE = ("mpt-status", "-p")
CMD_STATUS = ("mpt-status", "-n", "-i")

# __print_volume_advanced
# ioc:%d vol_id:%d type:%s raidlevel:%s num_disks:%d size(GB):%d state:%s flags: FLAG1 FLAG2 ...
# https://regex101.com/r/bfwEwE/1
VOLUME_LINE_RE = re.compile(
    r"^"
    r"\s*ioc:\s*(?P<controller_id>\d+)"
    r"\s+vol_id:\s*(?P<volume_id>\d+)"
    r"\s+type:\s*(?P<type>\w+)"
    r"\s+raidlevel:\s*(?P<raidlevel>[-\w]+)"
    r"\s+num_disks:\s*(?P<num_disks>\d+)"
    r"\s+size\(GB\):\s*(?P<size>\d+)"
    r"\s+state:\s*(?P<state>\w+)"
    r"\s+flags:(?P<flags>.+)",
    re.IGNORECASE,
)

VOLUME_STATES = ("OPTIMAL", "DEGRADED", "FAILED", "UNKNOWN")
VOLUME_FLAGS = ("ENABLED", "QUIESCED", "RESYNC_IN_PROGRESS", "VOLUME_INACTIVE")

# __print_physdisk_advanced
# ioc:%d %s:%d scsi_id:%d vendor:%s product_id:%s revision:%s size(GB):%d state:%s flags: FLAG1 FLAG2 ... sync_state: %s
#         ^ spare_id or phys_id                                                                                        ^ "n/a" for spare or number for phys
# ASC/ASCQ and SMART ASC/ASCQ fields are ignored here
# https://regex101.com/r/VpVY60/1
PHYS_LINE_RE = re.compile(
    r"^"
    r"\s*ioc:\s*(?P<controller_id>\d+)"
    r"\s+phys_id:\s*(?P<phys_id>\d+)"
    r"\s+scsi_id:\s*(?P<scsi_id>\d+)"
    r"\s+vendor:\s*(?P<vendor>\S+)"
    r"\s+product_id:\s*(?P<product_id>\S+)"
    r"\s+revision:\s*(?P<revision>\S+)"
    r"\s+size\(GB\):\s*(?P<size>\d+)"
    r"\s+state:\s*(?P<state>\w+)"
    r"\s+flags:(?P<flags>.+)"
    r"\s+sync_state:\s*(?P<sync_state>\d+)",
    re.IGNORECASE,
)

# https://regex101.com/r/iCiCYg/1
SPARE_LINE_RE = re.compile(
    r"^"
    r"\s*ioc:\s*(?P<controller_id>\d+)"
    r"\s+spare_id:\s*(?P<spare_id>\d+)"
    r"\s+scsi_id:\s*(?P<scsi_id>\d+)"
    r"\s+vendor:\s*(?P<vendor>\S+)"
    r"\s+product_id:\s*(?P<product_id>\S+)"
    r"\s+revision:\s*(?P<revision>\S+)"
    r"\s+size\(GB\):\s*(?P<size>\d+)"
    r"\s+state:\s*(?P<state>\w+)"
    r"\s+flags:(?P<flags>.+)"
    r"\s+sync_state:\s*n/a",
    re.IGNORECASE,
)

DISK_STATES = (
    "ONLINE",
    "MISSING",
    "NOT_COMPATIBLE",
    "FAILED",
    "INITIALIZING",
    "OFFLINE_REQUESTED",
    "FAILED_REQUESTED",
    "OTHER_OFFLINE",
    "UNKNOWN",
)
DISK_FLAGS = ("OUT_OF_SYNC", "QUIESCED")


class LineParseError(Exception):
    pass


class UnrecognizedLine(Exception):
    pass


class Metric(namedtuple("Metric", ["name", "type", "help", "value", "labels"])):
    __slots__ = ()

    @property
    def type_help_header(self):
        return "# HELP {o.name} {o.help}\n# TYPE {o.name} {o.type}".format(o=self)

    def __str__(self):
        # keys are sorted to help docstrip tests and for geenral consistensy
        keys = sorted(self.labels.keys())
        labels = ", ".join(('{}="{}"'.format(k, self.labels[k]) for k in keys))
        if labels:
            labels = "{" + labels + "}"
        return "{o.name}{} {o.value}".format(labels, o=self)


mpt_status_state = Metric("mpt_status_state", "gauge", "device state", None, {})
mpt_status_flag = Metric("mpt_status_flag", "gauge", "device flags", None, {})
mpt_status_disks_num = Metric(
    "mpt_status_disks_num", "gauge", "number of disks in logical volume", None, {}
)
mpt_status_sync_percentage = Metric(
    "mpt_status_sync_percentage", "gauge", "sync status", None, {}
)
mpt_status_size_gib = Metric(
    "mpt_status_size_gib", "gauge", "capacity of device in gibibytes", None, {}
)


def parse_volume_line(line):
    """
    Tries to parse line against VOLUME_LINE_RE and return metrics for logical volume.

    >>> for m in parse_volume_line("ioc:0 vol_id:10 type:IM raidlevel:RAID-1 num_disks:2 size(GB):135 state: OPTIMAL flags: ENABLED"):
    ...   print(m)
    mpt_status_state{controller_id="0", device="logical", id="10", raidlevel="RAID-1", state="OPTIMAL", type="IM"} 1
    mpt_status_state{controller_id="0", device="logical", id="10", raidlevel="RAID-1", state="DEGRADED", type="IM"} 0
    mpt_status_state{controller_id="0", device="logical", id="10", raidlevel="RAID-1", state="FAILED", type="IM"} 0
    mpt_status_state{controller_id="0", device="logical", id="10", raidlevel="RAID-1", state="UNKNOWN", type="IM"} 0
    mpt_status_flag{controller_id="0", device="logical", flag="ENABLED", id="10", raidlevel="RAID-1", type="IM"} 1
    mpt_status_flag{controller_id="0", device="logical", flag="QUIESCED", id="10", raidlevel="RAID-1", type="IM"} 0
    mpt_status_flag{controller_id="0", device="logical", flag="RESYNC_IN_PROGRESS", id="10", raidlevel="RAID-1", type="IM"} 0
    mpt_status_flag{controller_id="0", device="logical", flag="VOLUME_INACTIVE", id="10", raidlevel="RAID-1", type="IM"} 0
    mpt_status_size_gib{controller_id="0", device="logical", id="10", raidlevel="RAID-1", type="IM"} 135
    mpt_status_disks_num{controller_id="0", device="logical", id="10", raidlevel="RAID-1", type="IM"} 2
    """
    m = VOLUME_LINE_RE.match(line)
    if not m:
        raise LineParseError()
    base_labels = {
        "controller_id": m.group("controller_id"),
        "device": "logical",
        "id": m.group("volume_id"),
        "type": m.group("type"),
        "raidlevel": m.group("raidlevel"),
    }
    flags = m.group("flags").split()

    metrics = []
    for state in VOLUME_STATES:
        labels = base_labels.copy()
        labels["state"] = state
        value = 1 if m.group("state") == state else 0
        metrics.append(mpt_status_state._replace(value=value, labels=labels))
    for flag in VOLUME_FLAGS:
        labels = base_labels.copy()
        labels["flag"] = flag
        value = 1 if flag in flags else 0
        metrics.append(mpt_status_flag._replace(value=value, labels=labels))
    metrics.append(
        mpt_status_size_gib._replace(value=m.group("size"), labels=base_labels.copy())
    )
    metrics.append(
        mpt_status_disks_num._replace(
            value=m.group("num_disks"), labels=base_labels.copy()
        )
    )
    return metrics


def parse_phys_line(line):
    """
    Tries to parse line against PHYS_LINE_RE and return metrics for physical disk.

    >>> for m in parse_phys_line("ioc:0 phys_id:1 scsi_id:12 vendor:IBM-ESXS product_id:ST3146356SS      revision:BA49 size(GB):136 state: ONLINE flags: NONE sync_state: 100 ASC/ASCQ:0x11/0x00 SMART ASC/ASCQ:0x5d/0x00"):
    ...   print(m)
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="ONLINE", vendor="IBM-ESXS"} 1
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="MISSING", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="NOT_COMPATIBLE", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="FAILED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="INITIALIZING", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="OFFLINE_REQUESTED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="FAILED_REQUESTED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="OTHER_OFFLINE", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="UNKNOWN", vendor="IBM-ESXS"} 0
    mpt_status_flag{controller_id="0", device="physical", flag="OUT_OF_SYNC", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 0
    mpt_status_flag{controller_id="0", device="physical", flag="QUIESCED", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 0
    mpt_status_sync_percentage{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 100
    mpt_status_size_gib{controller_id="0", device="physical", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 136
    """
    m = PHYS_LINE_RE.match(line)
    if not m:
        raise LineParseError()
    base_labels = {
        "controller_id": m.group("controller_id"),
        "device": "physical",
        "id": m.group("phys_id"),
        "scsi_id": m.group("scsi_id"),
        "vendor": m.group("vendor"),
        "product_id": m.group("product_id"),
        "revision": m.group("revision"),
    }
    flags = m.group("flags").split()

    metrics = []
    for state in DISK_STATES:
        labels = base_labels.copy()
        labels["state"] = state
        value = 1 if m.group("state") == state else 0
        metrics.append(mpt_status_state._replace(value=value, labels=labels))
    for flag in DISK_FLAGS:
        labels = base_labels.copy()
        labels["flag"] = flag
        value = 1 if flag in flags else 0
        metrics.append(mpt_status_flag._replace(value=value, labels=labels))
    metrics.append(
        mpt_status_sync_percentage._replace(
            value=m.group("sync_state"), labels=base_labels.copy()
        )
    )
    metrics.append(
        mpt_status_size_gib._replace(value=m.group("size"), labels=base_labels.copy())
    )
    return metrics


def parse_spare_line(line):
    """
    Tries to parse line against SPARE_LINE_RE and return metrics for spare physical disk.

    >>> for m in parse_spare_line("ioc:0 spare_id:1 scsi_id:12 vendor:IBM-ESXS product_id:ST3146356SS      revision:BA49 size(GB):136 state: ONLINE flags: NONE sync_state: n/a ASC/ASCQ:0x11/0x00 SMART ASC/ASCQ:0x5d/0x00"):
    ...   print(m)
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="ONLINE", vendor="IBM-ESXS"} 1
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="MISSING", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="NOT_COMPATIBLE", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="FAILED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="INITIALIZING", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="OFFLINE_REQUESTED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="FAILED_REQUESTED", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="OTHER_OFFLINE", vendor="IBM-ESXS"} 0
    mpt_status_state{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", state="UNKNOWN", vendor="IBM-ESXS"} 0
    mpt_status_flag{controller_id="0", device="spare", flag="OUT_OF_SYNC", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 0
    mpt_status_flag{controller_id="0", device="spare", flag="QUIESCED", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 0
    mpt_status_size_gib{controller_id="0", device="spare", id="1", product_id="ST3146356SS", revision="BA49", scsi_id="12", vendor="IBM-ESXS"} 136
    """
    m = SPARE_LINE_RE.match(line)
    if not m:
        raise LineParseError()
    base_labels = {
        "controller_id": m.group("controller_id"),
        "device": "spare",
        "id": m.group("spare_id"),
        "scsi_id": m.group("scsi_id"),
        "vendor": m.group("vendor"),
        "product_id": m.group("product_id"),
        "revision": m.group("revision"),
    }
    flags = m.group("flags").split()

    metrics = []
    for state in DISK_STATES:
        labels = base_labels.copy()
        labels["state"] = state
        value = 1 if m.group("state") == state else 0
        metrics.append(mpt_status_state._replace(value=value, labels=labels))
    for flag in DISK_FLAGS:
        labels = base_labels.copy()
        labels["flag"] = flag
        value = 1 if flag in flags else 0
        metrics.append(mpt_status_flag._replace(value=value, labels=labels))
    metrics.append(
        mpt_status_size_gib._replace(value=m.group("size"), labels=base_labels.copy())
    )
    return metrics


def parse_line(line):
    "Try to parse line using functions in this module or raise UnrecognizedLine"
    for parse_fn in (parse_volume_line, parse_phys_line, parse_spare_line):
        try:
            return parse_fn(line)
        except LineParseError:
            pass
    raise UnrecognizedLine()


def main():
    def error(*args):
        print("ERROR:", *args, file=sys.stderr)

    def fatal(*args):
        print("FATAL:", *args, file=sys.stderr)
        sys.exit(1)

    # we call subprocess.run() so that bytes are returned (without text=True)
    # for compatibility with python 3.5
    try:
        probe_result = run(CMD_PROBE, stdout=PIPE, timeout=CMD_TIMEOUT, check=True)
    except CalledProcessError as e:
        fatal("Failed to probe controllers:\n" + e.output)
    except TimeoutExpired as e:
        fatal("Command '" + " ".join(CMD_PROBE) + "' timed out:\n" + e.output)
    except OSError as e:
        fatal("Can't invoke mpt-status: {}".format(e))

    probe_result_out = probe_result.stdout.decode(errors="replace")
    volume_ids = []
    for line in probe_result_out.splitlines():
        m = re.search(r"found\s+scsi\s+id=\s*(\d+)", line, re.IGNORECASE)
        if m:
            volume_ids.append(m.group(1))

    # When there were no devices, CMD_PROBE will fail:
    # https://github.com/baruch/mpt-status/blob/master/mpt-status.c#L1200
    # This check is here for 'just in case'
    if not volume_ids:
        fatal("No devices were found")

    metrics = {}
    for volume_id in volume_ids:
        cmd = CMD_STATUS + (volume_id,)
        try:
            status_result = run(cmd, stdout=PIPE, timeout=CMD_TIMEOUT, check=True)
        except CalledProcessError as e:
            fatal("Failed to check status for device " + volume_id + "\n" + e.output)
        except TimeoutExpired as e:
            fatal("Command '" + " ".join(cmd) + "' timed out:\n" + e.output)

        status_result_out = status_result.stdout.decode(errors="replace")
        for num, line in enumerate(status_result_out.splitlines()):
            if re.match(r"scsi_id:\d+\s+\d+%", line, re.IGNORECASE):
                # skip scsi_id:0 100% lines
                continue
            try:
                for metric in parse_line(line):
                    metrics.setdefault(metric.name, []).append(metric)
            except UnrecognizedLine:
                error("Can't recognize line #{}: {}".format(num, line))

    if not metrics:
        fatal("No metrics were parsed")

    for name in sorted(metrics.keys()):
        print(metrics[name][0].type_help_header)
        for metric_str in sorted(map(str, metrics[name])):
            print(metric_str)

    sys.exit(0)


if __name__ == "__main__":
    main()

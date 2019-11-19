#!/usr/bin/env python3
"""
This program formats output of sas2ircu tool as prometheus metrics

To test run
python -m doctest sas2ircu.py
"""

import sys
import re
from subprocess import run, PIPE, CalledProcessError, TimeoutExpired
from collections import namedtuple


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


sas2ircu_state_ok = Metric(
    "sas2ircu_state_ok", "gauge", "reports whether device state is ok", None, {}
)

CMD_TIMEOUT = 30  # seconds
CMD_PROBE = ("sas2ircu", "LIST")
CMD_DISPLAY = ("sas2ircu", "DISPLAY")
DISPLAY_CMD_STRUCT = (
    re.compile(r".*", re.IGNORECASE),  # preamble
    re.compile(r"^\s*Controller\s+information\s*$", re.IGNORECASE),
    re.compile(r".*", re.IGNORECASE),
    re.compile(r"^\s*IR\s+Volume\s+information\s*$", re.IGNORECASE),
    re.compile(r".*", re.IGNORECASE),
    re.compile(r"^\s*Physical\s+device\s+information\s*$", re.IGNORECASE),
    re.compile(r".*", re.IGNORECASE),
    re.compile(r"^\s*Enclosure\s+information\s*$", re.IGNORECASE),
    re.compile(r".*", re.IGNORECASE),
    re.compile(r".*", re.IGNORECASE),  # postamble
)

IR_VOLUME_REGEXP_MAP = (
    (re.compile(r"\s*IR\s+volume\s+(\d+)", re.IGNORECASE), "volume_num"),
    (re.compile(r"\s*Volume\s+ID\s*:\s*(\d+)", re.IGNORECASE), "volume_id"),
    (re.compile(r"\s*Status\s+of\s+volume\s*:(.+)", re.IGNORECASE), "state"),
    (re.compile(r"\s*RAID\s+level\s*:\s*(\w+)", re.IGNORECASE), "raidlevel"),
)

IR_VOLUME_OK_STATE = re.compile(r"(Inactive,\s*)?Okay\s*\(OKY\)", re.IGNORECASE)

PHYS_DEVICE_REGEXP_MAP = (
    (re.compile(r"\s*Enclosure\s+#\s*:\s*(\d+)", re.IGNORECASE), "enclosure"),
    (re.compile(r"\s*Slot\s+#\s*:\s*(\d+)", re.IGNORECASE), "slot"),
    (re.compile(r"\s*State\s*:(.+)", re.IGNORECASE), "state"),
    (re.compile(r"\s*Model\s+Number\s*:\s*(\w[-\w\s]+?)\s*$", re.IGNORECASE), "model"),
)

PHYS_DEVICE_OK_STATE = (
    re.compile(r"Optimal\s*\(OPT\)", re.IGNORECASE),
    re.compile(r"Ready\s*\(RDY\)", re.IGNORECASE),
)

PHYS_DEVICE_SKIP_PARTS = (
    re.compile(r"Initiator at ID #", re.IGNORECASE),
    re.compile(r"Device is a Enclosure services device", re.IGNORECASE),
)


class ParseError(Exception):
    pass


def split_display_sections(text):
    """
    Splits sas2ircu <controller_id> display output into secitons and returns
    their content.
    Returns tuple (controller_sec, ir_volume_sect, physical_device_sect, enclosure_sect)
    In case of error raises ParseError

    >>> inp = '''
    ... Preamle
    ... ------------------------------------------------------------------------
    ... Controller information
    ... ------------------------------------------------------------------------
    ... Controller section contents
    ...   bla1
    ... ------------------------------------------------------------------------
    ... IR Volume information
    ... ------------------------------------------------------------------------
    ... ------------------------------------------------------------------------
    ... Physical device information
    ... ------------------------------------------------------------------------
    ... Physical device section contents
    ...   bla2
    ... ------------------------------------------------------------------------
    ... Enclosure information
    ... ------------------------------------------------------------------------
    ... Enclosure seciton contens
    ...   bla3
    ...   bla-bla
    ... ------------------------------------------------------------------------
    ... Postamble
    ... '''
    >>> sects = split_display_sections(inp)
    >>> len(sects) == 4
    True
    >>> for sect in sects:
    ...   print(sect, end='')
    Controller section contents
      bla1
    Physical device section contents
      bla2
    Enclosure seciton contens
      bla3
      bla-bla
    """
    parts = re.split(r"-{2,}\n", text)
    if len(parts) != len(DISPLAY_CMD_STRUCT):
        raise ParseError(
            "Expected {} sections but got {}".format(
                len(DISPLAY_CMD_STRUCT), len(parts)
            )
        )
    for regexp, part in zip(DISPLAY_CMD_STRUCT, parts):
        if not regexp.match(part):
            raise ParseError("Expected section {} but got {}".format(pattern, part))

    return tuple(parts[2::2])


def parse_ir_volume_sect(text):
    """
    Parses IR volume information section and return prometheus metrics

    >>> inp = '''
    ... IR volume 1
    ...   Volume ID                               : 79
    ...   Status of volume                        : Okay (OKY)
    ...   Volume wwid                             : 0db809246c9f0e2a
    ...   RAID level                              : RAID1
    ...   Size (in MB)                            : 476416
    ...   Physical hard disks                     :
    ...   PHY[0] Enclosure#/Slot#                 : 1:0
    ...   PHY[1] Enclosure#/Slot#                 : 1:1
    ...
    ... IR volume 2
    ...   Volume ID                               : 80
    ...   Status of volume                        : Inactive, Okay (OKY)
    ...   Volume wwid                             : 0db809246c9f0e2a
    ...   RAID level                              : RAID1
    ...   Size (in MB)                            : 476416
    ...   Physical hard disks                     :
    ...   PHY[0] Enclosure#/Slot#                 : 1:0
    ...   PHY[1] Enclosure#/Slot#                 : 1:1
    ... '''
    >>> for m in parse_ir_volume_sect(inp):
    ...   print(m)
    sas2ircu_state_ok{device="logical", raidlevel="RAID1", volume_id="79", volume_num="1"} 1
    sas2ircu_state_ok{device="logical", raidlevel="RAID1", volume_id="80", volume_num="2"} 1
    >>> inp = ''
    >>> for m in parse_ir_volume_sect(inp):
    ...   print(m)
    """
    parts = split_by_empty_line(text)

    metrics = []
    for part in parts:
        if len(part) == 0:
            continue
        labels = {"device": "logical"}
        value = None
        for line in part:
            for regex, attr in IR_VOLUME_REGEXP_MAP:
                m = regex.match(line)
                if m:
                    if attr == "state":
                        value = 1 if IR_VOLUME_OK_STATE.search(m.group(1)) else 0
                    else:
                        labels[attr] = m.group(1)
        if value is None or len(labels) - 1 != len(IR_VOLUME_REGEXP_MAP) - 1:
            raise ParseError(
                "Failed to parse all attributes of IR volume. labels: {} value: {} wanted: {}".format(
                    labels, value, list((x[1] for x in IR_VOLUME_REGEXP_MAP))
                )
            )
        metrics.append(sas2ircu_state_ok._replace(value=value, labels=labels))

    return metrics


def parse_phys_device_sect(text):
    """
    Parses Physical device information section and return prometheus metrics

    >>> inp = '''
    ... Initiator at ID #0
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 1
    ...   Slot #                                  : 0
    ...   SAS Address                             : 4433221-1-0700-0000
    ...   State                                   : Optimal (OPT)
    ...   Size (in MB)/(in sectors)               : 476940/976773167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : WDC WD5003ABYX-1
    ...   Firmware Revision                       : 1S02
    ...   Serial No                               : WDWMAYP2093279
    ...   GUID                                    : 50014ee002e7ef75
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 1
    ...   Slot #                                  : 1
    ...   SAS Address                             : 4433221-1-0600-0000
    ...   State                                   : Optimal (OPT)
    ...   Size (in MB)/(in sectors)               : 476940/976773167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : WDC WD5003ABYX-1
    ...   Firmware Revision                       : 1S02
    ...   Serial No                               : WDWMAYP2164857
    ...   GUID                                    : 50014ee0ad95e58e
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 2
    ...   Slot #                                  : 18
    ...   SAS Address                             : 5003048-0-01cb-dc5e
    ...   State                                   : Ready (RDY)
    ...   Size (in MB)/(in sectors)               : 1907729/3907029167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : ST2000NM0033-9ZM
    ...   Firmware Revision                       : SN04
    ...   Serial No                               : Z1X4L8SQ
    ...   GUID                                    : 5000c5007b0353d1
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 2
    ...   Slot #                                  : 19
    ...   SAS Address                             : 5003048-0-01cb-dc5f
    ...   State                                   : Ready (RDY)
    ...   Size (in MB)/(in sectors)               : 1907729/3907029167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : ST32000641ASTrailSpaces    
    ...   Firmware Revision                       : CC13
    ...   Serial No                               : 9WM1R94E
    ...   GUID                                    : 5000c50027382dfc
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... 
    ... Device is a Enclosure services device
    ...   Enclosure #                             : 2
    ...   Slot #                                  : 24
    ...   SAS Address                             : 5003048-0-01cb-dc7d
    ...   State                                   : Standby (SBY)
    ...   Manufacturer                            : LSI CORP
    ...   Model Number                            : SAS2X36         
    ...   Firmware Revision                       : 0717
    ...   Serial No                               : x36557230
    ...   GUID                                    : N/A
    ...   Protocol                                : SAS
    ...   Device Type                             : Enclosure services device
    ... '''
    >>> for m in parse_phys_device_sect(inp):
    ...   print(m)
    sas2ircu_state_ok{device="physical", enclosure="1", model="WDC WD5003ABYX-1", slot="0"} 1
    sas2ircu_state_ok{device="physical", enclosure="1", model="WDC WD5003ABYX-1", slot="1"} 1
    sas2ircu_state_ok{device="physical", enclosure="2", model="ST2000NM0033-9ZM", slot="18"} 1
    sas2ircu_state_ok{device="physical", enclosure="2", model="ST32000641ASTrailSpaces", slot="19"} 1
    >>> inp = ''
    >>> for m in parse_phys_device_sect(inp):
    ...   print(m)
    """
    parts = split_by_empty_line(text)

    metrics = []
    for part in parts:
        if any(r.match(part[0]) for r in PHYS_DEVICE_SKIP_PARTS):
            continue
        labels = {"device": "physical"}
        value = None
        for line in part:
            for regex, attr in PHYS_DEVICE_REGEXP_MAP:
                m = regex.match(line)
                if m:
                    if attr == "state":
                        if any(r.search(m.group(1)) for r in PHYS_DEVICE_OK_STATE):
                            value = 1
                        else:
                            value = 0
                    else:
                        labels[attr] = m.group(1)
        if value is None or len(labels) - 1 != len(PHYS_DEVICE_REGEXP_MAP) - 1:
            raise ParseError(
                "Failed to parse all attributes of physical device. labels: {} value: {} wanted: {} part: {}".format(
                    labels, value, list((x[1] for x in PHYS_DEVICE_REGEXP_MAP)), part
                )
            )
        metrics.append(sas2ircu_state_ok._replace(value=value, labels=labels))

    return metrics


def split_by_empty_line(text):
    """
    Splits input text in parts delimited by empty lines

    >>> inp = '''
    ... Line 1
    ... 
    ... Line 2
    ...   Something
    ... 
    ... 
    ... Line 3
    ... '''
    >>> for part in split_by_empty_line(inp):
    ...   print(part)
    ['Line 1']
    ['Line 2', '  Something']
    ['Line 3']
    >>> inp = '''Hello
    ... '''
    >>> for part in split_by_empty_line(inp):
    ...   print(part)
    ['Hello']
    >>> inp = 'Hello'
    >>> for part in split_by_empty_line(inp):
    ...   print(part)
    ['Hello']
    >>> inp = ''
    >>> for part in split_by_empty_line(inp):
    ...   print(part)
    """
    text_lines = text.splitlines()
    parts = []
    start = 0
    for i, line in enumerate(text_lines):
        if len(line) == 0:
            if i != start:
                parts.append(text_lines[start:i])
            start = i + 1
    else:
        if start != len(text_lines):
            parts.append(text_lines[start:])

    return parts


def parse_display(controller_id, text):
    """
    Tries to parse sas2ircu <controller_id> display output and return promethues metrics.
    In case of error raises InfoParseError

    >>> inp = '''
    ... LSI Corporation SAS2 IR Configuration Utility.
    ... Version 16.00.00.00 (2013.03.01) 
    ... Copyright (c) 2009-2013 LSI Corporation. All rights reserved. 
    ... 
    ... Read configuration has been initiated for controller 0
    ... ------------------------------------------------------------------------
    ... Controller information
    ... ------------------------------------------------------------------------
    ...   Controller type                         : SAS2008
    ...   BIOS version                            : 7.11.01.00
    ...   Firmware version                        : 7.15.04.00
    ...   Channel description                     : 1 Serial Attached SCSI
    ...   Initiator ID                            : 0
    ...   Maximum physical devices                : 39
    ...   Concurrent commands supported           : 2607
    ...   Slot                                    : 1
    ...   Segment                                 : 0
    ...   Bus                                     : 3
    ...   Device                                  : 0
    ...   Function                                : 0
    ...   RAID Support                            : Yes
    ... ------------------------------------------------------------------------
    ... IR Volume information
    ... ------------------------------------------------------------------------
    ... IR volume 1
    ...   Volume ID                               : 79
    ...   Status of volume                        : Okay (OKY)
    ...   Volume wwid                             : 0db809246c9f0e2a
    ...   RAID level                              : RAID1
    ...   Size (in MB)                            : 476416
    ...   Physical hard disks                     :
    ...   PHY[0] Enclosure#/Slot#                 : 1:0
    ...   PHY[1] Enclosure#/Slot#                 : 1:1
    ... ------------------------------------------------------------------------
    ... Physical device information
    ... ------------------------------------------------------------------------
    ... Initiator at ID #0
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 1
    ...   Slot #                                  : 0
    ...   SAS Address                             : 4433221-1-0700-0000
    ...   State                                   : Optimal (OPT)
    ...   Size (in MB)/(in sectors)               : 476940/976773167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : WDC WD5003ABYX-1
    ...   Firmware Revision                       : 1S02
    ...   Serial No                               : WDWMAYP2093279
    ...   GUID                                    : 50014ee002e7ef75
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... 
    ... Device is a Hard disk
    ...   Enclosure #                             : 1
    ...   Slot #                                  : 1
    ...   SAS Address                             : 4433221-1-0600-0000
    ...   State                                   : Optimal (OPT)
    ...   Size (in MB)/(in sectors)               : 476940/976773167
    ...   Manufacturer                            : ATA     
    ...   Model Number                            : WDC WD5003ABYX-1
    ...   Firmware Revision                       : 1S02
    ...   Serial No                               : WDWMAYP2164857
    ...   GUID                                    : 50014ee0ad95e58e
    ...   Protocol                                : SATA
    ...   Drive Type                              : SATA_HDD
    ... ------------------------------------------------------------------------
    ... Enclosure information
    ... ------------------------------------------------------------------------
    ...   Enclosure#                              : 1
    ...   Logical ID                              : 5782bcb0:631f3500
    ...   Numslots                                : 8
    ...   StartSlot                               : 0
    ... ------------------------------------------------------------------------
    ... SAS2IRCU: Command DISPLAY Completed Successfully.
    ... SAS2IRCU: Utility Completed Successfully.
    ... '''
    >>> for m in parse_display(123, inp):
    ...   print(m)
    sas2ircu_state_ok{controller_id="123", device="logical", raidlevel="RAID1", volume_id="79", volume_num="1"} 1
    sas2ircu_state_ok{controller_id="123", device="physical", enclosure="1", model="WDC WD5003ABYX-1", slot="0"} 1
    sas2ircu_state_ok{controller_id="123", device="physical", enclosure="1", model="WDC WD5003ABYX-1", slot="1"} 1
    """
    # controller and enclosure metrics are not handled in this program
    _, ir_volume_sect, phys_device_sect, _ = split_display_sections(text)
    metrics = parse_ir_volume_sect(ir_volume_sect) + parse_phys_device_sect(
        phys_device_sect
    )
    for metric in metrics:
        metric.labels["controller_id"] = controller_id
    return metrics


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
        fatal("Can't invoke sas2ircu: {}".format(e))

    probe_result_out = probe_result.stdout.decode(errors="replace")
    controller_ids = []
    for line in probe_result_out.splitlines():
        m = re.match(r"\s*(\d+)", line, re.IGNORECASE)
        if m:
            controller_ids.append(m.group(1))

    if not controller_ids:
        fatal("No controllers were found")

    metrics = {}
    for controller_id in controller_ids:
        cmd = list(CMD_DISPLAY)
        cmd[1:1] = [controller_id]
        try:
            display_result = run(cmd, stdout=PIPE, timeout=CMD_TIMEOUT, check=True)
        except CalledProcessError as e:
            fatal(
                "Failed to check status for device " + controller_id + "\n" + e.output
            )
        except TimeoutExpired as e:
            fatal("Command '" + " ".join(cmd) + "' timed out:\n" + e.output)

        display_result_out = display_result.stdout.decode(errors="replace")
        try:
            for metric in parse_display(controller_id, display_result_out):
                metrics.setdefault(metric.name, []).append(metric)
        except ParseError as e:
            fatal("Failed to parse controller #{} display: {}".format(controller_id, e))

    if not metrics:
        fatal("No metrics were parsed")

    for name in sorted(metrics.keys()):
        print(metrics[name][0].type_help_header)
        for metric_str in sorted(map(str, metrics[name])):
            print(metric_str)

    sys.exit(0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import contextlib
import json
import os
import re
import sys

from middlewared.utils import MIDDLEWARE_RUN_DIR


def main():
    collectd_file = os.path.join(MIDDLEWARE_RUN_DIR, '.collectdalert')
    data = {}
    with contextlib.suppress(FileNotFoundError):
        with open(collectd_file, 'r') as f:
            try:
                data = json.loads(f.read())
            except Exception:
                pass

    text = sys.stdin.read().replace('\n\n', '\nMessage: ', 1)
    v = dict(re.findall(r"(?P<name>.*?): (?P<value>.*?)\n", text))

    k = v["Plugin"]
    if "PluginInstance" in list(v.keys()):
        k += "-" + v["PluginInstance"]
    k += "/" + v["Type"]
    if "TypeInstance" in list(v.keys()):
        k += "-" + v["TypeInstance"]

    if v["Severity"] == "OKAY":
        data.pop(k, None)
    else:
        data[k] = v

    with open(collectd_file, 'w') as f:
        f.write(json.dumps(data))


if __name__ == '__main__':
    main()

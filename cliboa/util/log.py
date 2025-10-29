#
# Copyright BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
import logging
import os
import sys
from typing import List

from cliboa import state


def _get_logger(modname: str):
    """
    Get logger

    Args:
        modname (str): module name
    Returns:
        logger instance
    """
    return logging.getLogger(modname)


_sys_uniq_path = []
_last_sys_path_count = -1


def _generate_sys_uniq_path() -> List[str]:
    seen = set()
    abs_paths = []
    for path in sys.path:
        if not path:
            continue
        abs_path = os.path.abspath(path)
        if abs_path not in seen:
            seen.add(abs_path)
            abs_paths.append(abs_path)

    paths_to_remove = set()

    for path1 in abs_paths:
        if path1 in paths_to_remove:
            continue
        for path2 in abs_paths:
            if path1 == path2 or path2 in paths_to_remove:
                continue

            longer_path, shorter_path = (None, None)

            if path1.startswith(path2) and len(path1) > len(path2):
                longer_path, shorter_path = path1, path2
            elif path2.startswith(path1) and len(path2) > len(path1):
                longer_path, shorter_path = path2, path1
            else:
                continue

            if longer_path.endswith("site-packages"):
                continue

            diff_path = os.path.relpath(longer_path, shorter_path)
            diff_parts = diff_path.split(os.sep)
            if any(part.startswith(".") for part in diff_parts if part):
                continue

            paths_to_remove.add(longer_path)

    uniq_paths = [p for p in abs_paths if p not in paths_to_remove]
    return uniq_paths


def _create_module_path(record_path) -> str:
    global _sys_uniq_path, _last_sys_path_count
    current_sys_path_count = len(sys.path)
    if current_sys_path_count != _last_sys_path_count:
        _sys_uniq_path = _generate_sys_uniq_path()
        _last_sys_path_count = current_sys_path_count

    rel_path = None
    for sys_path in _sys_uniq_path:
        if record_path.startswith(os.path.join(sys_path, "")):
            rel_path = record_path[len(sys_path) :].lstrip(os.sep)
            break

    if not rel_path:
        rel_path = record_path.lstrip(os.sep)

    module_path, _ = os.path.splitext(rel_path)
    parts = module_path.split(os.sep)
    if len(parts) >= 2 and parts[-1] == "__init__":
        parts.pop()

    s_parts = []
    full_flg = False
    last_i = len(parts) - 1
    for i, p in enumerate(parts):
        if full_flg:
            s_parts.append(p)
            full_flg = False
        elif p == "project" and i < last_i:
            s_parts.append(p[0])
            full_flg = True
        elif i == 0 and p == "common":
            s_parts.append(p[0])
        elif i == 0 or i == last_i:
            s_parts.append(p)
        else:
            s_parts.append(p[0])
    return ".".join(s_parts)


class CliboaLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # default values
        self.cliboaState = "_Unknown"
        self.cliboaPath = self.module
        # set values
        try:
            if not self.processName or self.processName == "MainProcess":
                self.cliboaState = str(state)
            else:
                self.cliboaState = self.processName + "." + str(state)
            self.cliboaPath = _create_module_path(self.pathname)
        except Exception:
            pass

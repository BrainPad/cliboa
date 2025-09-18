import logging
import os
import sys
from typing import List

from cliboa import state

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
        rel_path = record_path

    module_path, _ = os.path.splitext(rel_path)
    parts = module_path.split(os.sep)

    if len(parts) > 2:
        shortened_parts = [parts[0]]
        shortened_parts += [p[0] for p in parts[1:-1]]
        shortened_parts.append(parts[-1])
        return ".".join(shortened_parts)
    else:
        return ".".join(parts)


class CliboaLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.modulePath = _create_module_path(self.pathname)
        self.cliboaState = str(state)

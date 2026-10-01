#!/usr/bin/env python3
"""Deliberate test capture only: translate inotifywait events and raw file reads."""
import base64
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import time

root, output = Path(sys.argv[1]), Path(sys.argv[2])
output.mkdir(parents=True, exist_ok=True)
# Only synthetic playground namespaces, never arbitrary node logs.
selected = sorted(root.glob("capture_*")) + sorted(root.glob("other_*"))
if not selected:
    raise SystemExit("no capture pod directories")
watch = subprocess.Popen(
    ["inotifywait", "-m", "-r", "-e", "create,modify,close_write,moved_from,moved_to,delete",
     "--format", "%e|%w%f", *map(str, selected)],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
while "Watches established" not in watch.stderr.readline():
    if watch.poll() is not None:
        raise SystemExit("inotifywait failed establishing watches")

start = time.monotonic()
files = {}  # relative path -> open raw file; open descriptors survive real rotation
known_dirs = set()
ops = (output / "operations.jsonl").open("w")
events = (output / "inotify.log").open("w")


def emit(op, path, **fields):
    ops.write(json.dumps(dict(at_ms=int((time.monotonic() - start) * 1000),
                             op=op, path=path, **fields), separators=(",", ":")) + "\n")
    ops.flush()


def parents(path):
    for directory in reversed(path.parents):
        if directory == Path("."):
            continue
        name = str(directory)
        if name not in known_dirs:
            emit("mkdir", name)
            known_dirs.add(name)


def read_file(name):
    handle = files[name]
    data = handle.read()
    if data:
        emit("append", name, data=base64.b64encode(data).decode())


def discover(path):
    name = str(path.relative_to(root))
    if path.is_dir():
        parents(Path(name) / "placeholder")
        for child in sorted(path.iterdir()):
            discover(child)
    elif path.is_file() and not path.is_symlink() and name not in files:
        parents(Path(name))
        handle = path.open("rb")
        # A moved-to file is the same observed inode, not a fabricated rotation.
        inode = os.fstat(handle.fileno()).st_ino
        old = next((n for n, f in files.items()
                    if os.fstat(f.fileno()).st_ino == inode), None)
        if old is not None:
            handle.close()
            read_file(old)
            emit("rename", old, to=name)
            files[name] = files.pop(old)
        else:
            files[name] = handle
            emit("create", name, data=base64.b64encode(handle.read()).decode())


try:
    # Real preexisting backlog, read after native watches are established.
    for directory in selected:
        discover(directory)
    (output / "ready").touch()
    selector = selectors.DefaultSelector()
    selector.register(watch.stdout, selectors.EVENT_READ)
    pending = b""
    while time.monotonic() - start < 300:
        if not selector.select(timeout=min(1, max(0, 300 - (time.monotonic() - start)))):
            if watch.poll() is not None:
                raise RuntimeError("inotifywait stopped during capture")
            continue
        chunk = os.read(watch.stdout.fileno(), 65536)
        if not chunk:
            raise RuntimeError("inotifywait output closed")
        pending += chunk
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            event, raw_path = line.decode().split("|", 1)
            events.write(f"{int((time.monotonic()-start)*1000)}|{event}|{raw_path}\n")
            path = Path(raw_path.rstrip("/"))
            name = str(path.relative_to(root))
            if "MOVED_FROM" in event:
                if name in files:
                    read_file(name)
            elif "DELETE" in event:
                if name in files:
                    read_file(name)
                    files.pop(name).close()
                    emit("remove", name)
            elif path.exists():
                discover(path)
                if name in files:
                    read_file(name)
    for name in list(files):
        read_file(name)
finally:
    watch.terminate()
    watch.wait(timeout=5)
    for handle in files.values():
        handle.close()
    ops.close()
    events.close()
    (output / "ready").unlink(missing_ok=True)

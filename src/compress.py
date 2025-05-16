#!/usr/bin/env python3
import os
import time
import argparse
import h5py
import shutil
import tempfile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# -----------------------------------------------------------------------------
# Handler that compresses new .h5 files and tracks last event time
# -----------------------------------------------------------------------------
class H5Handler(FileSystemEventHandler):
    def __init__(self, compression, compression_level, last_event_time):
        super().__init__()
        self.compression = compression
        self.compression_level = compression_level
        self.last_event_time = last_event_time

    def is_stable(self, path, wait=1.0):
        """Return True once file size stops changing."""
        size1 = os.path.getsize(path)
        time.sleep(wait)
        size2 = os.path.getsize(path)
        return size1 == size2

    def compress_and_replace(self, src_path):
        """Compress src_path to a temp file then atomically replace original."""
        dirname, fname = os.path.split(src_path)
        # create temp file next to original
        fd, tmp_path = tempfile.mkstemp(suffix=".h5", dir=dirname)
        os.close(fd)

        print(f"[INFO] Compressing {src_path} → {tmp_path}")
        with h5py.File(src_path, 'r') as src, h5py.File(tmp_path, 'w') as dst:
            def _recurse(name, obj):
                if isinstance(obj, h5py.Dataset):
                    data = obj[()]
                    kwargs = {"compression": self.compression}
                    if self.compression == "gzip":
                        kwargs["compression_opts"] = self.compression_level
                    dst.create_dataset(name, data=data, **kwargs)
                else:
                    dst.create_group(name)
            src.visititems(_recurse)

        # atomic replace
        shutil.move(tmp_path, src_path)
        print(f"[DONE] Replaced original with compressed: {src_path}")

    def on_created(self, event):
        if event.is_directory:
            return
        filename = os.path.basename(event.src_path)
        if not filename.endswith(".h5") or filename.startswith("tmp"):
            return

        # mark event
        self.last_event_time[0] = time.time()
        filepath = event.src_path
        # wait until writing is done
        while not self.is_stable(filepath):
            print(f"[WAIT] Still writing: {filepath}")
            time.sleep(0.5)
        try:
            self.compress_and_replace(filepath)
        except Exception as e:
            print(f"[ERROR] Compression failed for {filepath}: {e}")

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Watch a directory (recursively) for new .h5 files, compress in-place, and exit after idle timeout."
    )
    p.add_argument(
        "--workdir", "-w", required=True,
        help="Root directory to watch for .h5 files."
    )
    p.add_argument(
        "--idle-timeout", "-t", type=float, default=300.0,
        help="Seconds to wait without new files before exiting (default: 10s)."
    )
    p.add_argument(
        "--compression", choices=("gzip","lzf","None"), default="gzip",
        help="h5py compression filter to use (default: gzip)."
    )
    p.add_argument(
        "--level", "-l", type=int, default=4,
        help="Compression level for gzip (ignored for lzf/None)."
    )
    p.add_argument(
        "--once", "-o", action="store_true",
        help="Compress all existing .h5 files recursively and exit (skip watching)."
    )
    args = p.parse_args()

    if not os.path.isdir(args.workdir):
        raise SystemExit(f"Error: workdir '{args.workdir}' does not exist or is not a directory.")

    # Shared mutable to track last event timestamp
    last_event_time = [time.time()]
    handler = H5Handler(
        compression=args.compression if args.compression!="None" else None,
        compression_level=args.level,
        last_event_time=last_event_time
    )

    # Option: compress all existing files and exit
    if args.once:
        for root, _, files in os.walk(args.workdir):
            for fname in files:
                if not fname.endswith(".h5") or fname.startswith("tmp"):
                    continue
                path = os.path.join(root, fname)
                try:
                    handler.compress_and_replace(path)
                except Exception as e:
                    print(f"[ERROR] Compression failed for {path}: {e}")
        return

    # Otherwise, start watching
    observer = Observer()
    observer.schedule(handler, args.workdir, recursive=True)
    observer.start()
    print(f"[WATCHING] {args.workdir!r} (idle timeout: {args.idle_timeout}s)")

    try:
        while True:
            time.sleep(1)
            idle = time.time() - last_event_time[0]
            if idle > args.idle_timeout:
                print(f"[IDLE] No new files for {idle:.1f}s; shutting down.")
                break
    except KeyboardInterrupt:
        print("[INTERRUPT] Stopping early on user request.")
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()


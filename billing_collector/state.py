import fcntl
import json
import os
import types
from typing import Self


class CheckpointState:
    """
    A context manager persisting the last successfully billed window's end time.

    Held open for the lifetime of the collector's run loop: locks the state file on
    entry, loads whatever was last written, and each call to mark_next_run_time
    flushes immediately so progress survives a crash rather than only being saved
    on a clean exit.
    """

    def __init__(self, file_location: str) -> None:
        self.file_location = file_location
        self.next_run_time: str | None = None

    def __enter__(self) -> Self:
        directory = os.path.dirname(self.file_location) or "."
        os.makedirs(directory, exist_ok=True)

        # Locked separately from the data file itself, since _flush() replaces the data
        # file's inode on every write and a lock held on a replaced inode no longer
        # excludes other processes opening the new one.
        self.lock_f = open(f"{self.file_location}.lock", "w")
        fcntl.flock(self.lock_f, fcntl.LOCK_EX)

        try:
            with open(self.file_location) as f:
                data = json.load(f)
                self.next_run_time = data.get("next_run_time")
        except (FileNotFoundError, json.JSONDecodeError):
            self.next_run_time = None
        return self

    def mark_next_run_time(self, next_run_time_iso: str) -> None:
        """Record next_run_time as the last successfully processed window end, and flush to disk."""
        self.next_run_time = next_run_time_iso
        self._flush()

    def _flush(self) -> None:
        directory = os.path.dirname(self.file_location) or "."
        tmp_path = f"{self.file_location}.tmp"
        with open(tmp_path, "w") as tmp_f:
            json.dump({"next_run_time": self.next_run_time}, tmp_f, indent=2)
            tmp_f.flush()
            os.fsync(tmp_f.fileno())
        os.replace(tmp_path, self.file_location)

        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        fcntl.flock(self.lock_f, fcntl.LOCK_UN)
        self.lock_f.close()

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
        if not os.path.exists(self.file_location):
            os.makedirs(os.path.dirname(self.file_location) or ".", exist_ok=True)
            with open(self.file_location, "w") as f:
                json.dump({"next_run_time": None}, f)

        self.f = open(self.file_location, "r+")
        fcntl.flock(self.f, fcntl.LOCK_EX)
        try:
            self.f.seek(0)
            data = json.load(self.f)
            self.next_run_time = data.get("next_run_time")
        except json.JSONDecodeError:
            self.next_run_time = None
        return self

    def mark_next_run_time(self, next_run_time_iso: str) -> None:
        """Record next_run_time as the last successfully processed window end, and flush to disk."""
        self.next_run_time = next_run_time_iso
        self._flush()

    def _flush(self) -> None:
        self.f.seek(0)
        json.dump({"next_run_time": self.next_run_time}, self.f, indent=2)
        self.f.truncate()
        self.f.flush()
        os.fsync(self.f.fileno())

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        fcntl.flock(self.f, fcntl.LOCK_UN)
        self.f.close()

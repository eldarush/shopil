"""Own Windows component descendants even after their original process exits."""

import ctypes
from ctypes import wintypes as w
import subprocess
import sys
import time


class Limits(ctypes.Structure):
    _fields_ = [("process_time", ctypes.c_longlong), ("job_time", ctypes.c_longlong),
                ("flags", w.DWORD), ("min_working_set", ctypes.c_size_t),
                ("max_working_set", ctypes.c_size_t), ("process_limit", w.DWORD),
                ("affinity", ctypes.c_size_t), ("priority", w.DWORD), ("scheduling", w.DWORD)]


class ExtendedLimits(ctypes.Structure):
    _fields_ = [("basic", Limits), ("io", ctypes.c_ulonglong * 6),
                ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t)]


class Accounting(ctypes.Structure):
    _fields_ = [("times", ctypes.c_longlong * 4), ("faults", w.DWORD),
                ("total", w.DWORD), ("active", w.DWORD), ("terminated", w.DWORD)]


def _api():
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateJobObjectW": ([ctypes.c_void_p, w.LPCWSTR], w.HANDLE),
        "SetInformationJobObject": ([w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD], w.BOOL),
        "QueryInformationJobObject": ([w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.c_void_p], w.BOOL),
        "AssignProcessToJobObject": ([w.HANDLE, w.HANDLE], w.BOOL),
        "GetCurrentProcess": ([], w.HANDLE),
        "TerminateJobObject": ([w.HANDLE, w.UINT], w.BOOL),
        "CloseHandle": ([w.HANDLE], w.BOOL),
    }
    for name, (args, result) in signatures.items():
        function = getattr(api, name)
        function.argtypes, function.restype = args, result
    return api


def _check(result):
    if not result:
        raise ctypes.WinError(ctypes.get_last_error())
    return result


class Job:
    def __init__(self):
        self.api = _api()
        self.handle = _check(self.api.CreateJobObjectW(None, None))
        limits = ExtendedLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        try:
            _check(self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)))
        except BaseException:
            self.api.CloseHandle(self.handle)
            self.handle = None
            raise

    def close(self) -> bool:
        """Terminate remaining members, wait boundedly, and release the job handle."""
        if self.handle is None:
            return False
        try:
            info = Accounting()
            def active():
                _check(self.api.QueryInformationJobObject(self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None))
                return info.active
            remaining = bool(active())
            if remaining:
                _check(self.api.TerminateJobObject(self.handle, 1))
                deadline = time.monotonic() + 5
                while active():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Windows component descendants did not terminate")
                    time.sleep(0.01)
            return remaining
        finally:
            handle = self.handle
            self.handle = None
            _check(self.api.CloseHandle(handle))


def main():
    # Join before launching the command: assignment after Popen races fast children.
    # Drop the inherited job handle so only the supervisor keeps the job alive.
    api = _api()
    handle = int(sys.argv[1])
    try:
        _check(api.AssignProcessToJobObject(handle, api.GetCurrentProcess()))
    finally:
        _check(api.CloseHandle(handle))
    return subprocess.call(sys.argv[2:], close_fds=True)


if __name__ == "__main__":
    raise SystemExit(main())

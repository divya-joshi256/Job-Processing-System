"""Lightweight Job Processing Framework."""

import threading
import time
import uuid
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, CancelledError


class JobStatus(str, Enum):
    PENDING = "PENDING"    # created, waiting for a free worker
    RUNNING = "RUNNING"    # actively executing right now
    COMPLETED = "COMPLETED"   # finished successfully, result available
    FAILED = "FAILED"        # finished with an exception
    CANCELLED = "CANCELLED"   # stopped before it ran


class JobType(str, Enum):
    IO_BOUND = "IO_BOUND"      # -> threads
    CPU_BOUND = "CPU_BOUND"    # -> processes

# The state machine: for each CURRENT status, which NEXT statuses are legal.
# This is what stops invalid jumps like COMPLETED -> RUNNING from ever
# silently happening -- every status change is checked against this table.
ALLOWED = {
    JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING: {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: set(),
    JobStatus.CANCELLED: set(),
}


@dataclass
class Job:
    id: str
    job_type: JobType
    status: JobStatus = JobStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    future: Optional[Future] = field(default=None, repr=False)


class JobRegistry:
    """Single lock guards every state change -> avoids race conditions."""
    """Holds every Job in a dict. Every read and write goes through one
        lock, so two threads can never corrupt a job's state by editing it at
        the same instant (e.g. a job finishing right as someone calls cancel())."""

    def __init__(self):
        self._lock = threading.RLock()
        self._jobs: Dict[str, Job] = {}

    def add(self, job: Job):
        with self._lock:
            self._jobs[job.id] = job

    def get(self, job_id: str) -> Job:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(job_id)
            return self._jobs[job_id]

    def try_transition(self, job_id: str, new_status: JobStatus, **fields) -> bool:
        with self._lock:
            job = self.get(job_id)
            if new_status not in ALLOWED[job.status]:
                return False
            job.status = new_status
            for k, v in fields.items():
                setattr(job, k, v)
            return True


class JobManager:
    def __init__(self, thread_workers=8, process_workers=4):
        self._reg = JobRegistry()
        self._thread_pool = ThreadPoolExecutor(max_workers=thread_workers)
        self._process_pool = ProcessPoolExecutor(max_workers=process_workers)

    def submit(self, func: Callable, *args, job_type=JobType.IO_BOUND, **kwargs) -> str:
        job_id = str(uuid.uuid4())
        self._reg.add(Job(id=job_id, job_type=job_type))

        if job_type == JobType.IO_BOUND:
            # Threads share memory, so a closure is fine -> mark RUNNING
            # exactly when the worker thread starts.
            def runner():
                self._reg.try_transition(job_id, JobStatus.RUNNING, started_at=time.time())
                return func(*args, **kwargs)
            future = self._thread_pool.submit(runner)
        else:
            # Processes require picklable targets -> no closure allowed.
            # Mark RUNNING here in the parent right after handoff instead.
            self._reg.try_transition(job_id, JobStatus.RUNNING, started_at=time.time())
            future = self._process_pool.submit(func, *args, **kwargs)

          # Save the Future on the Job so cancel()/result() can use it later.
        self._reg.get(job_id).future = future
        future.add_done_callback(lambda f: self._on_done(job_id, f))
        return job_id

    def _on_done(self, job_id: str, f: Future):
        if f.cancelled():
            self._reg.try_transition(job_id, JobStatus.CANCELLED, finished_at=time.time())
            return
        try:
            # f.result() re-raises any exception the job threw -- caught below.
            self._reg.try_transition(job_id, JobStatus.COMPLETED, result=f.result(), finished_at=time.time())
        except Exception as e:
            # Store a short, printable version of the exception rather than
            # the exception object itself (safer to store/inspect later).
            self._reg.try_transition(
                job_id, JobStatus.FAILED,
                error="".join(traceback.format_exception_only(type(e), e)).strip(),
                finished_at=time.time(),
            )

    def cancel(self, job_id: str) -> bool:
        job = self._reg.get(job_id)
        if job.status != JobStatus.PENDING:
            return False  # already running -> can't safely preempt
        if job.future and job.future.cancel():
            self._reg.try_transition(job_id, JobStatus.CANCELLED, finished_at=time.time())
            return True
        return False

    def status(self, job_id: str) -> str:
        return self._reg.get(job_id).status.value

    def result(self, job_id: str, timeout=None) -> Any:
        job = self._reg.get(job_id)
        try:
            return job.future.result(timeout=timeout)
        except CancelledError:
            raise RuntimeError(f"Job {job_id} was cancelled")

    def shutdown(self):
        self._thread_pool.shutdown()
        self._process_pool.shutdown()


# these must be defined at module level (NOT inside the
# `if __name__ == "__main__":` block below). On Windows, ProcessPoolExecutor
# workers re-import this file as a module to locate the function you
# submitted. They only see top-level code, so a function defined inside the
# __main__ guard is invisible to them -> "Can't get attribute 'x' on
# <module '__mp_main__'>".

def slow_add(a, b, delay=0.3):
    time.sleep(delay)
    return a + b


def cpu_heavy(n):
    return sum(i * i for i in range(n))


if __name__ == "__main__":
    m = JobManager(thread_workers=1, process_workers=2)
    j1 = m.submit(slow_add, 2, 3, delay=0.5, job_type=JobType.IO_BOUND)
    j2 = m.submit(cpu_heavy, 2_000_000, job_type=JobType.CPU_BOUND)
    j3 = m.submit(slow_add, 10, 20, job_type=JobType.IO_BOUND)  # queued behind j1

    # Check initial status
    print("j1 status:", m.status(j1))
    print("j2 status:", m.status(j2))
    print("j3 status:", m.status(j3))

    print("cancel j3:", m.cancel(j3), m.status(j3))
    print("j1:", m.result(j1), m.status(j1))
    print("j2:", m.result(j2), m.status(j2))
    m.shutdown()

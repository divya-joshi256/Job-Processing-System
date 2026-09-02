# Job Processing System

A lightweight Python job-processing framework designed to manage and execute different types of tasks efficiently.

The framework supports I/O-bound and CPU-bound jobs using thread and process pools. It also provides job status tracking, cancellation, result retrieval, error handling, and thread-safe job management.
## Features

* Submit, cancel, and track jobs
* Retrieve job results
* Thread-safe job management
* Error handling and status tracking

## Job Types

* **I/O-Bound:** Uses `ThreadPoolExecutor`
* **CPU-Bound:** Uses `ProcessPoolExecutor`

## Job States

`PENDING` → `RUNNING` → `COMPLETED`

Other states: `FAILED`, `CANCELLED`

## Concurrency & Safety

* Supports concurrent execution of multiple jobs.
* Uses threads for I/O-bound tasks and processes for CPU-bound tasks.
* `RLock` prevents race conditions during job state updates.

## Error Handling

* Exceptions are caught automatically.
* Failed jobs are marked `FAILED`.
* Error details are stored for inspection.

## Technologies

* Python 3
* Multithreading & Multiprocessing
* `concurrent.futures`
* Object-Oriented Programming

## How to Run

```bash
python job_processing_system_lite.py
```

## Sample Output

```text
j1 status: RUNNING
j2 status: RUNNING
j3 status: PENDING
cancel j3: True CANCELLED
j1: 5 COMPLETED
j2: 2666664666667000000 COMPLETED
```


## Future Improvements

* Job priorities
* Retry mechanism
* Progress tracking
* Logging and monitoring


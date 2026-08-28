# Persistent Priority Queue with Priority Aging & Observability Console

[![CI Test Suite](https://github.com/Twinkle1404/persistent-priority-queue/actions/workflows/test.yml/badge.svg)](https://github.com/Twinkle1404/persistent-priority-queue/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python implementation of a persistent priority queue satisfying the seven required SDE assignment operations, extended with anti-starvation **priority aging**, a REST API, automated testing, and a real-time web observability dashboard.

---

## 1. Quick Summary (2-Minute Scan)

- **WHAT**: A thread-safe, persistent priority queue (`module.py`) that serves high-urgency tasks first and persists state across process restarts.
- **WHY**: In-memory task queues lose all state on crashes. Furthermore, standard priority queues suffer from **task starvation** when high-priority tasks continuously arrive. Priority aging prevents starvation by dynamically increasing the urgency of older tasks.
- **HOW**: Implemented in Python standard library with atomic JSON file persistence (zero external dependencies), an anti-starvation dynamic aging formula, a Flask REST API, and a real-time observability console.

---

## 2. Assignment Requirements & Compliance Matrix

| Requirement | Implementation in `module.py` | Status | Notes |
|:---|:---|:---:|:---|
| `insert(item, priority)` | `PersistentPriorityQueue.insert()` | ✅ Yes | Validates finite numbers; rejects `NaN`/`Inf` |
| `extract_min` | `PersistentPriorityQueue.extract_min()` | ✅ Yes | Removes & returns lowest effective priority item |
| `extract_max` | `PersistentPriorityQueue.extract_max()` | ✅ Yes | Removes & returns highest effective priority item |
| `peek` | `PersistentPriorityQueue.peek(mode="min"\|"max")` | ✅ Yes | Inspects top item without removing |
| `update` | `PersistentPriorityQueue.update(id, prio, payload)` | ✅ Yes | Updates priority and/or payload (supports `None`) |
| `delete` | `PersistentPriorityQueue.delete(item_id)` | ✅ Yes | Removes item by string ID |
| `is_empty` | `PersistentPriorityQueue.is_empty()` | ✅ Yes | Returns boolean |
| **Persistence** | `JSONFileStorage` / `SQLiteStorage` | ✅ Yes | State persists across process restarts |
| **Module Independence** | `module.py` has **zero** Flask dependency | ✅ Yes | Usable purely via `from module import PersistentPriorityQueue` |

---

## 3. Core Architecture & Project Story

The project is structured in clean, progressive layers:

```
[Core Priority Queue (module.py)]
              ↓
  [Atomic JSON Persistence (fsync + replace)]
              ↓
  [Priority Aging Engine (Anti-Starvation)]
              ↓
  [Comprehensive Automated Test Suite (test_module.py)]
              ↓
  [REST API Layer (server.py)]
              ↓
  [Web Observability Console (templates/index.html)]
```

### Core Usage (`module.py`):

```python
from module import PersistentPriorityQueue

# Initialize queue with JSON file persistence and priority aging
pq = PersistentPriorityQueue(backend="json", storage_file="priority_queue.json", decay_rate=0.01)

pq.insert("task_payment", priority=10, payload="Process invoice #101")
pq.peek(mode="min")               # ('task_payment', 10.0, 'Process invoice #101')
pq.update("task_payment", priority=2)
pq.extract_min()                  # Removes & returns lowest effective priority item
pq.extract_max()                  # Removes & returns highest effective priority item
pq.delete("task_payment")
pq.is_empty()                     # True / False
```

*(Note: `from module import PriorityQueue` is also exported as an alias).*

---

## 4. Persistence Strategy & Durability

1. **JSON File Storage (Default & Primary Assignment Backend)**:
   - Primary persistence solution using temporary file writes + `os.fsync()` + atomic filesystem replacement (`os.replace`).
   - Designed to reduce the risk of partial file writes upon unexpected process termination.
   - If an existing persistence file is corrupted or unreadable, the loader raises `PersistenceCorruptionError` and **preserves the corrupted file on disk** rather than silently wiping queue data.
2. **PostgreSQL Storage (Optional / Experimental Backend)**:
   - Experimental relational backend for multi-worker environments.
   - Requires external PostgreSQL database and driver (`pip install psycopg[binary]`).
3. **SQLite Storage (Secondary / Local Backend)**:
   - Retained as a secondary file-backed relational driver for local evaluation.

> **Concurrency Scope**: Thread safety is provided within a single process using reentrant locks (`threading.RLock`). Multi-process or distributed consumers would require database-level transactional claiming/locking (such as `SELECT FOR UPDATE SKIP LOCKED`).

---

## 5. Time Complexity — Based on Implementation

| Operation | JSON File Storage (Default) | SQLite / PostgreSQL (Relational) | Space Complexity |
|:---|:---:|:---:|:---:|
| `insert(id, prio, payload)` | $O(1)$ memory dict write + $O(N)$ disk write | $O(\log N)$ B-Tree write | $O(1)$ per item |
| `extract_min()` | $O(N)$ aging scan + $O(N)$ disk write | Index scan / table evaluation* | $O(1)$ |
| `extract_max()` | $O(N)$ aging scan + $O(N)$ disk write | Index scan / table evaluation* | $O(1)$ |
| `peek(mode)` | $O(N)$ aging scan | $O(1)$ / table evaluation* | $O(1)$ |
| `update(id, prio, payload)` | $O(1)$ dict lookup + $O(N)$ disk write | $O(\log N)$ primary key update | $O(1)$ |
| `delete(id)` | $O(1)$ dict delete + $O(N)$ disk write | $O(\log N)$ primary key delete | $O(1)$ |
| `is_empty()` | $O(1)$ | $O(1)$ | $O(1)$ |
| `size()` | $O(1)$ | $O(1)$ | $O(1)$ |

### Data Structure Rationalization:
The JSON backend uses an in-memory dictionary for persistent task records. Priority selection performs a linear scan because effective priority changes continuously with task age. This design favors simple persistence and arbitrary ID-based update/delete operations over heap-level extraction complexity.

### Dynamic Priority Aging & Database Query Complexity:
- When **`decay_rate = 0.0`** (static priorities): Database backends leverage the `(priority ASC, seq ASC)` B-Tree index for $O(\log N)$ min/max extraction.
- When **`decay_rate > 0.0`** (dynamic aging): Ordering is governed by the expression `priority - decay_rate * (now - inserted_at)`. Because dynamic time expressions shift relative ordering over time, the static `(priority, seq)` index cannot directly represent the dynamic sort order without row evaluation by the query planner.

---

## 6. Priority Aging & Anti-Starvation

### The Starvation Problem:
In standard priority queues, if high-priority tasks (e.g. priority `1.0`) continuously arrive, low-priority tasks (e.g. priority `100.0`) wait indefinitely in the queue and never execute (starvation).

### The Mathematical Solution:
$$\text{Effective Priority} = \text{Base Priority} - (\text{Decay Rate} \times \text{Age in Seconds})$$

Where:
- $\text{Base Priority}$: Initial numerical priority assigned at insertion time (finite float, lower = more urgent).
- $\text{Age in Seconds} = \text{Current Time} - \text{Insertion Timestamp}$.
- $\text{Decay Rate}$: Urgency increase rate per second ($\text{points/sec}$).

### Concrete Aging Scenario (`decay_rate = 5.0/s`):
- **Task A (Old batch job)**: Base priority = `100.0`, age = $10\text{ s} \implies \text{Effective} = 100.0 - (5.0 \times 10) = \mathbf{50.0}$
- **Task B (Newly arrived task)**: Base priority = `60.0`, age = $0\text{ s} \implies \text{Effective} = 60.0 - (5.0 \times 0) = \mathbf{60.0}$

**Result**: Task A's effective priority drops to `50.0`, overtaking Task B (`60.0`). Task A is extracted first by `extract_min()`, preventing starvation. Setting `decay_rate=0.0` reproduces standard priority queue behavior.

---

## 7. Canonical REST API Reference

The Flask application exposes a clean REST API:

| Method | Endpoint | Request Body | Description | Status Code |
|:---|:---|:---|:---|:---:|
| `GET` | `/api/health` | — | Lightweight health check verifying backend read availability | `200`, `500` |
| `GET` | `/api/stats` | — | Queue metrics (size, oldest age, decay rate, session extracted count*) | `200` |
| `GET` | `/api/queue` | — | All items sorted by effective priority | `200` |
| `POST` | `/api/insert` | `{"item_id": "...", "priority": 10, "payload": "..."}` | Insert task with finite priority | `201`, `400` |
| `POST` | `/api/extract-min` | — | Remove and return min effective priority task | `200`, `404` |
| `POST` | `/api/extract-max` | — | Remove and return max effective priority task | `200`, `404` |
| `GET` | `/api/peek?mode=min` | — | Inspect min/max without removal | `200`, `404` |
| `PUT` | `/api/update/<id>` | `{"priority": 5, "payload": "updated note"}` | Update existing task (priority and/or payload) | `200`, `404` |
| `DELETE` | `/api/delete/<id>` | — | Delete task by ID | `200`, `404` |
| `POST` | `/api/clear` | — | Clear all tasks from queue | `200` |

*\*Note: `extracted_count` is a session-level in-memory metric that resets on server restart.*

### Consistent JSON Error Format:
All error responses return structured JSON:
```json
{
  "success": false,
  "error": "Priority must be a finite number"
}
```

---

## 8. Setup & Execution Guide

### Prerequisites:
- Python 3.10+ (Tested on Python 3.10, 3.11, 3.12, 3.13)

### Local Execution (Standard Path):

```bash
# 1. Create and activate virtual environment
python -m venv .venv
# On Windows: .venv\Scripts\activate | On Linux/macOS: source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run complete automated test suite
python -m pytest test_module.py -v --tb=short

# 4. Run CLI demonstration script
python example.py

# 5. Launch live web dashboard
python server.py
# Visit http://localhost:5000 in your browser
```

### Docker Execution (Optional):

The Docker configuration mounts a persistent volume to `/app/data` with `PQ_STORAGE_FILE=/app/data/priority_queue.json`, protecting queue state across container restarts:

```bash
# Build and start container
docker compose up --build

# Verify in browser at http://localhost:5000
```

---

## 9. Technical Interview Talking Points (SDE Q&A)

1. **What is a Priority Queue?**
   *An Abstract Data Type where each element has a priority score and items are extracted based on urgency rather than insertion order.*
2. **Why persistence?**
   *Standard in-memory heaps lose all state upon process crashes. Persistence guarantees task durability by writing state to disk upon every mutation.*
3. **How does priority aging prevent starvation?**
   *Older tasks gain priority over time via $\text{Effective} = \text{Base} - (\text{Decay} \times \text{Age})$. This ensures low-priority tasks eventually bubble up and execute.*
4. **How are crash writes prevented from corrupting files?**
   *We write to a `.tmp` file, flush to OS buffers with `f.flush()`, commit to disk with `os.fsync()`, and perform an atomic filesystem rename (`os.replace`).*
5. **What happens if a persistence file is corrupted?**
   *The loader detects JSON corruption, raises `PersistenceCorruptionError`, and preserves the original file on disk without overwriting it.*
6. **How are equal-priority ties resolved?**
   *A monotonic sequence counter initialized higher than existing items provides deterministic First-In, First-Out (FIFO) ordering for items managed by the queue instance.*
7. **What is the complexity of each operation?**
   *JSON backend: $O(1)$ memory lookup, $O(N)$ linear aging evaluation scan, and $O(N)$ full JSON disk serialization. Relational backends (SQLite/PostgreSQL): $O(\log N)$ B-Tree index operations when priorities are static (`decay_rate = 0.0`), but dynamic aging (`decay_rate > 0.0`) evaluates `priority - decay_rate * age`, requiring row evaluation by the query planner.*
8. **Why validate for finite numbers?**
   *`float("nan")` and `float("inf")` break comparison logic and sort invariants in Python and databases. We reject non-finite values at the API and library boundaries.*
9. **How is thread safety achieved?**
   *All read/write operations and persistence flushes are synchronized using reentrant locks (`threading.RLock`).*
10. **How would you scale this to 1 million tasks?**
    *Switch backend to PostgreSQL with indexed tables, partition active vs. completed tasks, or integrate an LSM-tree storage engine (e.g. RocksDB) for high-throughput persistent keys.*

---

## 10. Project Structure

```
Persistent priority queue/
│
├── module.py               # Core Priority Queue implementation & storage drivers
├── server.py               # Flask REST API server and observability router
├── test_module.py          # Automated unit, persistence, aging & API tests
├── example.py              # CLI demonstration & persistence walkthrough
├── requirements.txt        # Python package dependencies (Flask, Pytest)
├── Dockerfile              # Container definition for deployment
├── docker-compose.yml      # Multi-container orchestration config
├── .gitignore              # Git ignore rules
│
├── .github/
│   └── workflows/
│       └── test.yml        # Multi-Python CI test pipeline
│
└── templates/
    └── index.html          # Web Observability Console UI
```

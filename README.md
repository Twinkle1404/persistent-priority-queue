# Persistent Priority Queue with Priority Aging & Observability Console

[![CI Test Suite](https://github.com/your-username/persistent-priority-queue/actions/workflows/test.yml/badge.svg)](https://github.com/your-username/persistent-priority-queue/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A clean, thread-safe Priority Queue in Python supporting core operations (`insert`, `extract_min`, `extract_max`, `peek`, `update`, `delete`, `is_empty`) with crash-durable file persistence, anti-starvation **priority aging / decay**, a REST API, and a real-time web observability dashboard.

---

## 1. Quick Summary (2-Minute Scan)

- **What was built**: A persistent priority queue module (`module.py`) that serves highest-urgency tasks first and survives process restarts without data loss.
- **Why it was built**: Solves task loss during crashes (via atomic disk persistence) and prevents low-priority task starvation (via dynamic priority aging).
- **What makes it interesting**:
  - Pluggable storage architecture: **JSON file-based storage** (default assignment-compliant backend), **PostgreSQL** (optional relational DB backend), and **SQLite** (secondary local backend).
  - Real-time Observability Dashboard + REST API with live search, sorting, and decay visualizers.
  - Zero external dependencies for core queue operations.

---

## 2. Core Operations & API

The module exposes the 7 required methods:

```python
from module import PersistentPriorityQueue

pq = PersistentPriorityQueue(backend="json", storage_file="priority_queue.json", decay_rate=0.01)

pq.insert("task_payment", priority=10, payload="Process invoice #101")
pq.peek(mode="min")               # ('task_payment', 10.0, 'Process invoice #101')
pq.update("task_payment", priority=2)
pq.extract_min()                  # Extracts min effective priority item
pq.extract_max()                  # Extracts max effective priority item
pq.delete("task_payment")
pq.is_empty()                     # True / False
```

---

## 3. Persistence Strategy

In accordance with assignment guidelines:

1. **JSON File Storage (Default & Assignment-Compliant)**:
   - Primary persistence solution using atomic filesystem writes (`.tmp` + `os.replace`).
   - Requires zero external database setup and runs out of the box on any system.
2. **PostgreSQL Storage (Optional Relational Backend)**:
   - Production relational database backend connecting via standard PostgreSQL DSN.
3. **SQLite Storage (Secondary / Local Backend)**:
   - File-backed relational storage using B-Tree indexing and WAL journaling for local benchmarks.

> **Note**: JSON is the primary submission backend. SQLite is retained solely as a local secondary utility.

---

## 4. Algorithmic Complexity (Honest & Measured)

Complexity breakdown based on the actual implementation:

| Operation | JSON File Storage (Default) | SQLite / PostgreSQL (B-Tree Indexed) |
|:---|:---:|:---:|
| `insert(id, prio, payload)` | $O(1)$ memory dict write + $O(N)$ disk flush | $O(\log N)$ B-Tree write |
| `extract_min()` | $O(N)$ linear scan (aging evaluation) + $O(N)$ disk flush | $O(\log N)$ B-Tree scan & delete |
| `extract_max()` | $O(N)$ linear scan (aging evaluation) + $O(N)$ disk flush | $O(\log N)$ B-Tree scan & delete |
| `peek(mode)` | $O(N)$ linear scan (aging evaluation) | $O(1)$ / $O(\log N)$ |
| `update(id, prio, payload)` | $O(1)$ dict lookup + $O(N)$ disk flush | $O(\log N)$ primary key update |
| `delete(id)` | $O(1)$ dict delete + $O(N)$ disk flush | $O(\log N)$ primary key delete |
| `is_empty()` | $O(1)$ | $O(1)$ |
| `size()` | $O(1)$ | $O(1)$ |

### Why this design?
- In an in-memory priority queue, a **binary heap** provides $O(\log N)$ insert/extract, but $O(N)$ ID lookups for `update`/`delete`.
- For persistent file storage, `JSONFileStorage` provides $O(1)$ ID lookups in memory with atomic disk writes, making it transparent and simple to reason about.
- For high-volume workloads, `PostgreSQLStorage` scales with $O(\log N)$ B-Tree indexes directly on disk.

---

## 5. Priority Aging & Anti-Starvation

### Formula:
$$\text{Effective Priority} = \text{Base Priority} - (\text{Decay Rate} \times \text{Age in Seconds})$$

When `decay_rate > 0`, older items gradually decrease in numerical priority score (= higher urgency for `extract_min`), preventing low-priority tasks from starving.

### Concrete Scenario (`decay_rate = 5.0/s`):
- **Task A**: Base priority = `100.0`, age = $10\text{ s} \implies \text{Effective} = 100.0 - (5.0 \times 10) = \mathbf{50.0}$
- **Task B**: Base priority = `60.0`, age = $0\text{ s} \implies \text{Effective} = 60.0 - (5.0 \times 0) = \mathbf{60.0}$

**Result**: Task A overtakes Task B and is extracted first. Setting `decay_rate=0.0` preserves standard priority queue ordering.

---

## 6. Architecture & System Design

```
┌─────────────────────────────────────────────────────────────┐
│              Live Web Observability Console                 │
│                 (templates/index.html)                      │
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTP / REST API
┌──────────────────────────────▼──────────────────────────────┐
│                    Flask API Server                         │
│                      (server.py)                            │
│   /api/health  /api/stats  /api/queue  /api/extract-min     │
└──────────────────────────────┬──────────────────────────────┘
                               │ Python API
┌──────────────────────────────▼──────────────────────────────┐
│              PersistentPriorityQueue (module.py)            │
│         - Monotonic Sequence (Strict FIFO Tie-Breaking)     │
│         - Priority Aging Engine                             │
│         - Thread Safety (threading.RLock)                   │
└──────────────────────────────┬──────────────────────────────┘
                               │ Storage Backend Adapter
             ┌─────────────────┼─────────────────┐
             ▼                 ▼                 ▼
   ┌───────────────────┐ ┌───────────┐ ┌───────────────────┐
   │ JSONFileStorage   │ │  SQLite   │ │ PostgreSQLStorage │
   │ (Primary Default) │ │ (Secondary│ │(Optional Relation)│
   │  Atomic Renames   │ │  B-Tree)  │ │   ACID Storage    │
   └───────────────────┘ └───────────┘ └───────────────────┘
```

---

## 7. REST API Reference

| Method | Endpoint | Request Body | Description |
|:---|:---|:---|:---|
| `GET` | `/api/health` | — | System health and backend status |
| `GET` | `/api/stats` | — | Operational metrics (size, oldest age, extracted count*) |
| `GET` | `/api/queue` | — | All items sorted by effective priority |
| `POST` | `/api/insert` | `{"item_id", "priority", "payload"}` | Insert new task |
| `POST` | `/api/extract-min` | — | Extract min effective priority item |
| `POST` | `/api/extract-max` | — | Extract max effective priority item |
| `GET` | `/api/peek?mode=min` | — | Inspect min/max without removal |
| `PUT` | `/api/update/<id>` | `{"priority", "payload"}` | Update task |
| `DELETE` | `/api/delete/<id>` | — | Delete task by ID |
| `POST` | `/api/clear` | — | Clear queue |

*\*Note: `extracted_count` is a session-level in-memory metric.*

---

## 8. Setup & Running Instructions

### Local Execution (Standard Path):

```bash
# 1. Create virtual environment
python -m venv .venv
# On Windows: .venv\Scripts\activate | On Linux/macOS: source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run complete test suite (52 tests)
python -m pytest test_module.py -v --tb=short

# 4. Run CLI demonstration script
python example.py

# 5. Launch live web dashboard
python server.py
# Visit http://localhost:5000 in your browser
```

### Optional Docker Execution:

```bash
docker compose up --build
```

---

## 9. Technical Interview Talking Points (SDE Q&A)

1. **What is a Priority Queue?**
   *An ADT where items are served by urgency rather than arrival order. Commonly implemented via binary heaps, trees, or indexed stores.*
2. **Why persistence?**
   *In-memory heaps lose state on process restarts. Persistence writes state to disk on mutation to prevent data loss.*
3. **How does priority aging prevent starvation?**
   *Low-priority tasks age over time, lowering their numerical priority score via $\text{Effective} = \text{Base} - (\text{Decay} \times \text{Age})$ so they eventually overtake newer tasks.*
4. **How are crash writes prevented from corrupting files?**
   *JSON writes first write to a temporary `.tmp` file, then execute an atomic filesystem rename (`os.replace`).*
5. **How are equal-priority ties resolved?**
   *A monotonic microsecond sequence counter ensures strict First-In, First-Out (FIFO) ordering for identical priority scores.*
6. **What is the complexity of each operation?**
   *JSON backend: $O(1)$ memory dictionary lookups, $O(N)$ scan for aging and disk flush. Database backends: $O(\log N)$ B-Tree operations.*
7. **How is thread safety achieved?**
   *All mutations and read operations are guarded with `threading.RLock()` across storage adapters.*
8. **How would you scale this to 1 million tasks?**
   *Switch backend to PostgreSQL with indexed tables, partition by status/priority, or use persistent log-structured merge-tree (LSM) engines (RocksDB).*

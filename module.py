"""
module.py - Persistent Priority Queue with Priority Aging

A production-grade, thread-safe, persistent priority queue supporting:
  - insert, extract_min, extract_max, peek, update, delete, is_empty
  - Priority decay / aging: old items automatically become more urgent over time
  - Pluggable storage backends: SQLite (file), JSON (file), PostgreSQL (database)

Priority Aging Formula:
  effective_priority = base_priority - (decay_rate × age_in_seconds)

  When decay_rate > 0, older items drift toward lower effective priority (= higher
  urgency for extract_min), preventing starvation of low-priority tasks.
  When decay_rate = 0 (default), behavior is identical to a standard priority queue.
"""

import abc
import json
import math
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union


_UNCHANGED = object()


class PersistenceCorruptionError(RuntimeError):
    """Raised when a persistence storage file is corrupted or unreadable."""
    pass


def validate_finite_priority(priority: Any) -> float:
    """Validate and convert priority to a finite float, rejecting NaN and Infinities."""
    try:
        p = float(priority)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Priority must be a valid number, got {priority!r}") from e
    if not math.isfinite(p):
        raise ValueError(f"Priority must be a finite number, got {priority!r}")
    return p


# ---------------------------------------------------------------------------
# Storage Backend Interface
# ---------------------------------------------------------------------------

class StorageBackend(abc.ABC):
    """Abstract base class defining persistent storage backend contract."""

    @abc.abstractmethod
    def insert(self, item_id: str, priority: float, payload: Any, seq: int, inserted_at: float) -> None:
        """Insert a new item with given id, priority, payload, sequence number, and insertion timestamp."""
        pass

    @abc.abstractmethod
    def extract_min(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        """Extract and return item dict with the lowest effective priority."""
        pass

    @abc.abstractmethod
    def extract_max(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        """Extract and return item dict with the highest effective priority."""
        pass

    @abc.abstractmethod
    def peek(self, mode: str = "min", decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        """Peek at item dict without removing it."""
        pass

    @abc.abstractmethod
    def update(self, item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
        """Update existing item's priority and/or payload. Returns True if updated."""
        pass

    @abc.abstractmethod
    def delete(self, item_id: str) -> bool:
        """Delete an item by item_id. Returns True if deleted."""
        pass

    @abc.abstractmethod
    def is_empty(self) -> bool:
        """Check if storage is empty."""
        pass

    @abc.abstractmethod
    def size(self) -> int:
        """Return total number of items in storage."""
        pass

    @abc.abstractmethod
    def get_all_items(self) -> List[Dict[str, Any]]:
        """Return all items as list of dicts with keys: item_id, priority, payload, seq, inserted_at."""
        pass

    @abc.abstractmethod
    def clear(self) -> None:
        """Clear all items from storage."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Close backend connections or resources."""
        pass


# ---------------------------------------------------------------------------
# SQLite File-Based Storage
# ---------------------------------------------------------------------------

class SQLiteStorage(StorageBackend):
    """
    File-based persistent storage backend powered by SQLite.
    Leverages B-Tree index on (priority, seq) for O(log N) operations.
    When decay_rate > 0, computes effective priority in SQL for correct ordering.
    """

    def __init__(self, db_path: str = "priority_queue.db", table_name: str = "priority_queue"):
        self.db_path = db_path
        self.table_name = table_name
        self._lock = threading.RLock()
        dir_name = os.path.dirname(os.path.abspath(self.db_path))
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._init_db()

    def _init_db(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    item_id TEXT PRIMARY KEY,
                    priority REAL NOT NULL,
                    payload TEXT,
                    seq INTEGER NOT NULL,
                    inserted_at REAL NOT NULL DEFAULT 0
                )
                """
            )
            self._conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_priority_seq
                ON {self.table_name} (priority ASC, seq ASC)
                """
            )
            # Migration: add inserted_at column if upgrading from old schema
            cursor = self._conn.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            if "inserted_at" not in columns:
                self._conn.execute(
                    f"ALTER TABLE {self.table_name} ADD COLUMN inserted_at REAL NOT NULL DEFAULT 0"
                )

    def insert(self, item_id: str, priority: float, payload: Any, seq: int, inserted_at: float) -> None:
        payload_json = json.dumps(payload)
        with self._lock, self._conn:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT 1 FROM {self.table_name} WHERE item_id = ?", (item_id,))
            if cursor.fetchone():
                raise ValueError(f"Item with id '{item_id}' already exists in priority queue.")
            cursor.execute(
                f"INSERT INTO {self.table_name} (item_id, priority, payload, seq, inserted_at) VALUES (?, ?, ?, ?, ?)",
                (item_id, float(priority), payload_json, seq, inserted_at),
            )

    def _build_order_sql(self, direction: str, decay_rate: float) -> str:
        """Build ORDER BY clause, using effective priority when decay is active."""
        if decay_rate == 0.0:
            return f"ORDER BY priority {direction}, seq ASC"
        else:
            # effective_priority = priority - (decay_rate * (now - inserted_at))
            now = time.time()
            return f"ORDER BY (priority - ({decay_rate} * ({now} - inserted_at))) {direction}, seq ASC"

    def extract_min(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock, self._conn:
            cursor = self._conn.cursor()
            order = self._build_order_sql("ASC", decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = ?", (item_id,))
            return {
                "item_id": item_id,
                "priority": priority,
                "payload": json.loads(payload_json),
                "seq": seq,
                "inserted_at": inserted_at,
            }

    def extract_max(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock, self._conn:
            cursor = self._conn.cursor()
            order = self._build_order_sql("DESC", decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = ?", (item_id,))
            return {
                "item_id": item_id,
                "priority": priority,
                "payload": json.loads(payload_json),
                "seq": seq,
                "inserted_at": inserted_at,
            }

    def peek(self, mode: str = "min", decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        mode_lower = mode.lower()
        if mode_lower not in ("min", "max"):
            raise ValueError("mode must be 'min' or 'max'")
        direction = "ASC" if mode_lower == "min" else "DESC"

        with self._lock:
            cursor = self._conn.cursor()
            order = self._build_order_sql(direction, decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            return {
                "item_id": item_id,
                "priority": priority,
                "payload": json.loads(payload_json),
                "seq": seq,
                "inserted_at": inserted_at,
            }

    def update(self, item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
        if priority is None and payload is _UNCHANGED:
            return False
        with self._lock, self._conn:
            cursor = self._conn.cursor()
            cursor.execute(
                f"SELECT priority, payload FROM {self.table_name} WHERE item_id = ?", (item_id,)
            )
            row = cursor.fetchone()
            if not row:
                return False
            curr_priority, curr_payload_json = row
            new_priority = float(priority) if priority is not None else curr_priority
            new_payload_json = json.dumps(payload) if payload is not _UNCHANGED else curr_payload_json
            cursor.execute(
                f"UPDATE {self.table_name} SET priority = ?, payload = ? WHERE item_id = ?",
                (new_priority, new_payload_json, item_id),
            )
            return cursor.rowcount > 0

    def delete(self, item_id: str) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.cursor()
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = ?", (item_id,))
            return cursor.rowcount > 0

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            return cursor.fetchone()[0]

    def get_all_items(self) -> List[Dict[str, Any]]:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} ORDER BY priority ASC, seq ASC"
            )
            items = []
            for row in cursor.fetchall():
                item_id, priority, payload_json, seq, inserted_at = row
                items.append({
                    "item_id": item_id,
                    "priority": priority,
                    "payload": json.loads(payload_json),
                    "seq": seq,
                    "inserted_at": inserted_at,
                })
            return items

    def clear(self) -> None:
        with self._lock, self._conn:
            self._conn.execute(f"DELETE FROM {self.table_name}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# ---------------------------------------------------------------------------
# JSON File-Based Storage
# ---------------------------------------------------------------------------

class JSONFileStorage(StorageBackend):
    """
    Direct file-based persistent storage using JSON with atomic file updates.
    Maintains items in-memory and flushes atomically to disk on every mutation.
    """

    def __init__(self, file_path: str = "priority_queue.json"):
        self.file_path = file_path
        self._lock = threading.RLock()
        self._items: Dict[str, Dict[str, Any]] = {}
        dir_name = os.path.dirname(os.path.abspath(self.file_path))
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        self._load()

    def _load(self) -> None:
        with self._lock:
            if not os.path.exists(self.file_path):
                self._items = {}
                return

            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                raise PersistenceCorruptionError(
                    f"Corrupted persistence file at '{self.file_path}': invalid JSON format ({e}). "
                    "The corrupted file has been preserved on disk without modification."
                ) from e
            except Exception as e:
                raise PersistenceCorruptionError(
                    f"Failed to read persistence file at '{self.file_path}': {e}. "
                    "The file has been preserved on disk without modification."
                ) from e

            if not isinstance(data, dict):
                raise PersistenceCorruptionError(
                    f"Corrupted storage schema: root element in '{self.file_path}' must be a JSON object/dict, "
                    f"got {type(data).__name__}. The corrupted file has been preserved on disk."
                )

            self._items = data

    def _save(self) -> None:
        with self._lock:
            temp_path = f"{self.file_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self._items, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.file_path)

    @staticmethod
    def _eff_priority(item: Dict[str, Any], decay_rate: float) -> float:
        if decay_rate == 0.0:
            return item["priority"]
        age = time.time() - item.get("inserted_at", time.time())
        return item["priority"] - (decay_rate * age)

    def insert(self, item_id: str, priority: float, payload: Any, seq: int, inserted_at: float) -> None:
        with self._lock:
            if item_id in self._items:
                raise ValueError(f"Item with id '{item_id}' already exists in priority queue.")
            self._items[item_id] = {
                "item_id": item_id,
                "priority": float(priority),
                "payload": payload,
                "seq": seq,
                "inserted_at": inserted_at,
            }
            self._save()

    def extract_min(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._items:
                return None
            min_item = min(
                self._items.values(),
                key=lambda x: (self._eff_priority(x, decay_rate), x["seq"]),
            )
            del self._items[min_item["item_id"]]
            self._save()
            return dict(min_item)

    def extract_max(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._items:
                return None
            max_item = max(
                self._items.values(),
                key=lambda x: (self._eff_priority(x, decay_rate), -x["seq"]),
            )
            del self._items[max_item["item_id"]]
            self._save()
            return dict(max_item)

    def peek(self, mode: str = "min", decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        mode_lower = mode.lower()
        if mode_lower not in ("min", "max"):
            raise ValueError("mode must be 'min' or 'max'")
        with self._lock:
            if not self._items:
                return None
            if mode_lower == "min":
                item = min(
                    self._items.values(),
                    key=lambda x: (self._eff_priority(x, decay_rate), x["seq"]),
                )
            else:
                item = max(
                    self._items.values(),
                    key=lambda x: (self._eff_priority(x, decay_rate), -x["seq"]),
                )
            return dict(item)

    def update(self, item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
        if priority is None and payload is _UNCHANGED:
            return False
        with self._lock:
            if item_id not in self._items:
                return False
            if priority is not None:
                self._items[item_id]["priority"] = float(priority)
            if payload is not _UNCHANGED:
                self._items[item_id]["payload"] = payload
            self._save()
            return True

    def delete(self, item_id: str) -> bool:
        with self._lock:
            if item_id not in self._items:
                return False
            del self._items[item_id]
            self._save()
            return True

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._items) == 0

    def size(self) -> int:
        with self._lock:
            return len(self._items)

    def get_all_items(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._items.values()]

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
            self._save()

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# PostgreSQL Database Storage
# ---------------------------------------------------------------------------

class PostgreSQLStorage(StorageBackend):
    """
    Relational database storage backend using PostgreSQL (optional backend).
    Install driver: pip install psycopg[binary]
    """

    def __init__(self, connection_dsn: str, table_name: str = "priority_queue"):
        self.dsn = connection_dsn
        self.table_name = table_name
        self._lock = threading.RLock()
        try:
            import psycopg
            self._psycopg = psycopg
        except ImportError:
            try:
                import psycopg2 as psycopg
                self._psycopg = psycopg
            except ImportError:
                raise ImportError(
                    "PostgreSQL driver not installed. Install with: pip install psycopg[binary]"
                )
        self._conn = self._psycopg.connect(self.dsn)
        self._conn.autocommit = True
        self._init_db()

    def _init_db(self) -> None:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    item_id TEXT PRIMARY KEY,
                    priority DOUBLE PRECISION NOT NULL,
                    payload TEXT,
                    seq BIGINT NOT NULL,
                    inserted_at DOUBLE PRECISION NOT NULL DEFAULT 0
                );
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table_name}_priority_seq
                ON {self.table_name} (priority ASC, seq ASC);
                """
            )

    def insert(self, item_id: str, priority: float, payload: Any, seq: int, inserted_at: float) -> None:
        payload_json = json.dumps(payload)
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {self.table_name} WHERE item_id = %s", (item_id,))
            if cursor.fetchone():
                raise ValueError(f"Item with id '{item_id}' already exists in priority queue.")
            cursor.execute(
                f"INSERT INTO {self.table_name} (item_id, priority, payload, seq, inserted_at) VALUES (%s, %s, %s, %s, %s)",
                (item_id, float(priority), payload_json, seq, inserted_at),
            )

    def _build_order_sql(self, direction: str, decay_rate: float) -> str:
        if decay_rate == 0.0:
            return f"ORDER BY priority {direction}, seq ASC"
        else:
            now = time.time()
            return f"ORDER BY (priority - ({decay_rate} * ({now} - inserted_at))) {direction}, seq ASC"

    def extract_min(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock, self._conn.cursor() as cursor:
            order = self._build_order_sql("ASC", decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = %s", (item_id,))
            return {"item_id": item_id, "priority": priority, "payload": json.loads(payload_json), "seq": seq, "inserted_at": inserted_at}

    def extract_max(self, decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        with self._lock, self._conn.cursor() as cursor:
            order = self._build_order_sql("DESC", decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = %s", (item_id,))
            return {"item_id": item_id, "priority": priority, "payload": json.loads(payload_json), "seq": seq, "inserted_at": inserted_at}

    def peek(self, mode: str = "min", decay_rate: float = 0.0) -> Optional[Dict[str, Any]]:
        mode_lower = mode.lower()
        if mode_lower not in ("min", "max"):
            raise ValueError("mode must be 'min' or 'max'")
        direction = "ASC" if mode_lower == "min" else "DESC"
        with self._lock, self._conn.cursor() as cursor:
            order = self._build_order_sql(direction, decay_rate)
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} {order} LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                return None
            item_id, priority, payload_json, seq, inserted_at = row
            return {"item_id": item_id, "priority": priority, "payload": json.loads(payload_json), "seq": seq, "inserted_at": inserted_at}

    def update(self, item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
        if priority is None and payload is _UNCHANGED:
            return False
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(f"SELECT priority, payload FROM {self.table_name} WHERE item_id = %s", (item_id,))
            row = cursor.fetchone()
            if not row:
                return False
            curr_priority, curr_payload_json = row
            new_priority = float(priority) if priority is not None else curr_priority
            new_payload_json = json.dumps(payload) if payload is not _UNCHANGED else curr_payload_json
            cursor.execute(
                f"UPDATE {self.table_name} SET priority = %s, payload = %s WHERE item_id = %s",
                (new_priority, new_payload_json, item_id),
            )
            return cursor.rowcount > 0

    def delete(self, item_id: str) -> bool:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE item_id = %s", (item_id,))
            return cursor.rowcount > 0

    def is_empty(self) -> bool:
        return self.size() == 0

    def size(self) -> int:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            return cursor.fetchone()[0]

    def get_all_items(self) -> List[Dict[str, Any]]:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(
                f"SELECT item_id, priority, payload, seq, inserted_at FROM {self.table_name} ORDER BY priority ASC, seq ASC"
            )
            items = []
            for row in cursor.fetchall():
                item_id, priority, payload_json, seq, inserted_at = row
                items.append({"item_id": item_id, "priority": priority, "payload": json.loads(payload_json), "seq": seq, "inserted_at": inserted_at})
            return items

    def clear(self) -> None:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {self.table_name}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()


# ---------------------------------------------------------------------------
# Main Persistent Priority Queue
# ---------------------------------------------------------------------------

class PersistentPriorityQueue:
    """
    Persistent Priority Queue with optional priority aging.

    Exposes the 7 required operations:
      insert, extract_min, extract_max, peek, update, delete, is_empty

    Plus additional utility:
      size, clear, close, get_all_items

    Priority Aging (anti-starvation):
      When decay_rate > 0, effective_priority = base_priority - (decay_rate * age_seconds).
      Older items drift toward lower effective priority, making them more urgent
      for extract_min. This prevents low-priority tasks from waiting forever.

    Storage backends:
      'json'     — Primary file-based JSON storage with atomic writes (zero dependencies)
      'sqlite'   — File-based SQLite relational storage
      'postgres' — PostgreSQL relational database (optional)
      StorageBackend instance — Custom backend
    """

    def __init__(
        self,
        backend: Union[str, StorageBackend] = "json",
        storage_file: Optional[str] = None,
        postgres_dsn: Optional[str] = None,
        table_name: str = "priority_queue",
        decay_rate: float = 0.0,
    ):
        """
        :param backend: Storage backend type ('json', 'sqlite', 'postgres') or StorageBackend instance.
        :param storage_file: File path for json/sqlite backends (defaults: 'priority_queue.json' or 'priority_queue.db').
        :param postgres_dsn: PostgreSQL connection DSN string.
        :param table_name: Table/collection name in the storage.
        :param decay_rate: Priority decay rate per second (0.0 = disabled).
        """
        self._lock = threading.RLock()
        self.decay_rate = float(decay_rate)

        if isinstance(backend, StorageBackend):
            self.backend = backend
        elif backend == "json":
            file_path = storage_file or "priority_queue.json"
            self.backend = JSONFileStorage(file_path=file_path)
        elif backend == "sqlite":
            db_path = storage_file or "priority_queue.db"
            self.backend = SQLiteStorage(db_path=db_path, table_name=table_name)
        elif backend == "postgres":
            if not postgres_dsn:
                raise ValueError("postgres_dsn must be provided when backend is 'postgres'")
            self.backend = PostgreSQLStorage(connection_dsn=postgres_dsn, table_name=table_name)
        else:
            raise ValueError(
                f"Unsupported backend '{backend}'. Use 'json', 'sqlite', 'postgres', or a StorageBackend instance."
            )

        # Monotonically seed sequence counter higher than any existing items loaded from disk
        existing_items = self.backend.get_all_items()
        max_existing_seq = max((item.get("seq", 0) for item in existing_items), default=0)
        self._seq_counter = max(int(time.time() * 1000000), max_existing_seq)

    def _next_seq(self) -> int:
        with self._lock:
            self._seq_counter += 1
            return self._seq_counter

    @staticmethod
    def compute_effective_priority(base_priority: float, inserted_at: float, decay_rate: float) -> float:
        """Compute effective priority accounting for aging decay."""
        if decay_rate == 0.0:
            return base_priority
        age = time.time() - inserted_at
        return base_priority - (decay_rate * age)

    # --- Core 7 Operations ---

    def insert(self, item_id: str, priority: float, payload: Any = None) -> None:
        """
        Insert an item into the persistent priority queue.

        :param item_id: Unique string identifier for the item.
        :param priority: Numerical priority (lower = more urgent for extract_min).
        :param payload: Optional serializable data attached to the item.
        """
        valid_prio = validate_finite_priority(priority)
        seq = self._next_seq()
        inserted_at = time.time()
        self.backend.insert(str(item_id), valid_prio, payload, seq, inserted_at)

    def extract_min(self) -> Optional[Tuple[str, float, Any]]:
        """
        Extract and remove the item with the lowest effective priority.

        :return: (item_id, priority, payload) tuple or None if empty.
        """
        result = self.backend.extract_min(decay_rate=self.decay_rate)
        if result is None:
            return None
        return (result["item_id"], result["priority"], result["payload"])

    def extract_max(self) -> Optional[Tuple[str, float, Any]]:
        """
        Extract and remove the item with the highest effective priority.

        :return: (item_id, priority, payload) tuple or None if empty.
        """
        result = self.backend.extract_max(decay_rate=self.decay_rate)
        if result is None:
            return None
        return (result["item_id"], result["priority"], result["payload"])

    def peek(self, mode: str = "min") -> Optional[Tuple[str, float, Any]]:
        """
        Peek at the min or max item without removing it.

        :param mode: 'min' or 'max' (default: 'min').
        :return: (item_id, priority, payload) tuple or None if empty.
        """
        result = self.backend.peek(mode=mode, decay_rate=self.decay_rate)
        if result is None:
            return None
        return (result["item_id"], result["priority"], result["payload"])

    def peek_min(self) -> Optional[Tuple[str, float, Any]]:
        """Convenience: peek at min-priority item."""
        return self.peek(mode="min")

    def peek_max(self) -> Optional[Tuple[str, float, Any]]:
        """Convenience: peek at max-priority item."""
        return self.peek(mode="max")

    def update(self, item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
        """
        Update an existing item's priority and/or payload.

        :param item_id: Item identifier.
        :param priority: New base priority (or None to keep current).
        :param payload: New payload (or _UNCHANGED to keep current, can be None).
        :return: True if updated, False if item not found.
        """
        if priority is None and payload is _UNCHANGED:
            return False
        valid_prio = validate_finite_priority(priority) if priority is not None else None
        return self.backend.update(
            str(item_id),
            valid_prio,
            payload,
        )

    def delete(self, item_id: str) -> bool:
        """
        Delete an item from the priority queue by its ID.

        :return: True if deleted, False if item did not exist.
        """
        return self.backend.delete(str(item_id))

    def is_empty(self) -> bool:
        """Check if the priority queue is empty."""
        return self.backend.is_empty()

    # --- Additional Utility ---

    def size(self) -> int:
        """Return total count of items in the queue."""
        return self.backend.size()

    def get_all_items(self) -> List[Dict[str, Any]]:
        """
        Return all items enriched with effective priority and age information.

        Each item dict contains:
          item_id, priority (base), effective_priority, payload, inserted_at, age_seconds
        """
        raw_items = self.backend.get_all_items()
        now = time.time()
        enriched = []
        for item in raw_items:
            age = now - item.get("inserted_at", now)
            eff = self.compute_effective_priority(
                item["priority"], item.get("inserted_at", now), self.decay_rate
            )
            enriched.append({
                "item_id": item["item_id"],
                "priority": item["priority"],
                "effective_priority": round(eff, 4),
                "payload": item["payload"],
                "inserted_at": item.get("inserted_at", 0),
                "age_seconds": round(age, 1),
                "seq": item.get("seq", 0),
            })
        # Sort by effective priority ascending (most urgent first), tie-breaking by sequence
        enriched.sort(key=lambda x: (x["effective_priority"], x.get("seq", 0)))
        return enriched

    def clear(self) -> None:
        """Clear all items from the queue."""
        self.backend.clear()

    def close(self) -> None:
        """Close storage connection."""
        self.backend.close()

    def __len__(self) -> int:
        return self.size()


# Alias for assignment flexibility (from module import PriorityQueue)
PriorityQueue = PersistentPriorityQueue

# ---------------------------------------------------------------------------
# Module-level convenience functions (backward compatible)
# ---------------------------------------------------------------------------

_default_instance: Optional[PersistentPriorityQueue] = None


def get_default_queue(
    backend: str = "json",
    storage_file: Optional[str] = None,
) -> PersistentPriorityQueue:
    global _default_instance
    if _default_instance is None:
        _default_instance = PersistentPriorityQueue(backend=backend, storage_file=storage_file)
    return _default_instance


def insert(item_id: str, priority: float, payload: Any = None) -> None:
    get_default_queue().insert(item_id, priority, payload)


def extract_min() -> Optional[Tuple[str, float, Any]]:
    return get_default_queue().extract_min()


def extract_max() -> Optional[Tuple[str, float, Any]]:
    return get_default_queue().extract_max()


def peek(mode: str = "min") -> Optional[Tuple[str, float, Any]]:
    return get_default_queue().peek(mode)


def update(item_id: str, priority: Optional[float] = None, payload: Any = _UNCHANGED) -> bool:
    return get_default_queue().update(item_id, priority, payload)


def delete(item_id: str) -> bool:
    return get_default_queue().delete(item_id)


def is_empty() -> bool:
    return get_default_queue().is_empty()


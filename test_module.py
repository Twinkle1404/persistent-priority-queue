"""
test_module.py - Comprehensive Unit, Integration, Aging, Persistence & API Tests

Test Suite Coverage:
  1. Core Priority Queue Operations (7 required methods)
  2. Persistence Across Restarts & Crash Recovery
  3. Priority Aging / Anti-Starvation Verification
  4. Top-level Module Helpers & Aliases
  5. Edge Cases & Boundary Conditions
  6. Flask REST API & Observability Endpoints
"""

import json
import os
import time
import pytest

import module
from module import PersistentPriorityQueue, PriorityQueue, SQLiteStorage, JSONFileStorage
from server import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sqlite_queue(tmp_path):
    db_file = str(tmp_path / "test_pq.db")
    pq = PersistentPriorityQueue(backend="sqlite", storage_file=db_file)
    yield pq
    pq.close()


@pytest.fixture
def json_queue(tmp_path):
    json_file = str(tmp_path / "test_pq.json")
    pq = PersistentPriorityQueue(backend="json", storage_file=json_file)
    yield pq
    pq.close()


@pytest.fixture
def sqlite_queue_with_decay(tmp_path):
    db_file = str(tmp_path / "test_pq_decay.db")
    pq = PersistentPriorityQueue(backend="sqlite", storage_file=db_file, decay_rate=50.0)
    yield pq
    pq.close()


@pytest.fixture
def json_queue_with_decay(tmp_path):
    json_file = str(tmp_path / "test_pq_decay.json")
    pq = PersistentPriorityQueue(backend="json", storage_file=json_file, decay_rate=50.0)
    yield pq
    pq.close()


@pytest.fixture
def api_client(tmp_path):
    """Flask test client fixture with isolated test database."""
    test_file = str(tmp_path / "api_test.json")
    app.config["TESTING"] = True
    # Re-initialize the server's global pq for clean test isolation
    import server
    server.pq = PersistentPriorityQueue(backend="json", storage_file=test_file, decay_rate=0.01)
    server._extracted_count = 0
    with app.test_client() as client:
        yield client
    server.pq.close()


# ---------------------------------------------------------------------------
# 1. Core Operations Tests (parameterized for both backends)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fixture_name", ["sqlite_queue", "json_queue"])
class TestCoreOperations:

    def test_is_empty_and_size_initial(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        assert pq.is_empty() is True
        assert pq.size() == 0
        assert len(pq) == 0

    def test_insert_and_peek(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_a", priority=10, payload={"name": "A"})
        assert pq.is_empty() is False
        assert pq.size() == 1

        min_item = pq.peek(mode="min")
        assert min_item == ("task_a", 10.0, {"name": "A"})

        max_item = pq.peek(mode="max")
        assert max_item == ("task_a", 10.0, {"name": "A"})

    def test_duplicate_insert_raises_error(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_a", priority=10)
        with pytest.raises(ValueError, match="already exists"):
            pq.insert("task_a", priority=20)

    def test_extract_min_ordering(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_mid", priority=50, payload="mid")
        pq.insert("task_low", priority=10, payload="low")
        pq.insert("task_high", priority=100, payload="high")

        assert pq.size() == 3

        item1 = pq.extract_min()
        assert item1 == ("task_low", 10.0, "low")

        item2 = pq.extract_min()
        assert item2 == ("task_mid", 50.0, "mid")

        item3 = pq.extract_min()
        assert item3 == ("task_high", 100.0, "high")

        assert pq.is_empty() is True
        assert pq.extract_min() is None

    def test_extract_max_ordering(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_mid", priority=50, payload="mid")
        pq.insert("task_low", priority=10, payload="low")
        pq.insert("task_high", priority=100, payload="high")

        item1 = pq.extract_max()
        assert item1 == ("task_high", 100.0, "high")

        item2 = pq.extract_max()
        assert item2 == ("task_mid", 50.0, "mid")

        item3 = pq.extract_max()
        assert item3 == ("task_low", 10.0, "low")

        assert pq.is_empty() is True
        assert pq.extract_max() is None

    def test_fifo_tie_breaking(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("first", priority=5, payload="1")
        pq.insert("second", priority=5, payload="2")
        pq.insert("third", priority=5, payload="3")

        # extract_min should yield in FIFO insertion order for ties
        assert pq.extract_min() == ("first", 5.0, "1")
        assert pq.extract_min() == ("second", 5.0, "2")
        assert pq.extract_min() == ("third", 5.0, "3")

    def test_update_priority_and_payload(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_x", priority=50, payload="old_payload")
        pq.insert("task_y", priority=20, payload="other")

        # Initially min is task_y (priority 20)
        assert pq.peek_min()[0] == "task_y"

        # Update task_x priority from 50 to 5
        updated = pq.update("task_x", priority=5, payload="new_payload")
        assert updated is True

        # Now min should be task_x (priority 5)
        min_item = pq.peek_min()
        assert min_item == ("task_x", 5.0, "new_payload")

        # Non-existent item update returns False
        assert pq.update("non_existent", priority=1) is False

    def test_delete(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_1", priority=10)
        pq.insert("task_2", priority=20)

        assert pq.size() == 2
        deleted = pq.delete("task_1")
        assert deleted is True
        assert pq.size() == 1
        assert pq.peek_min()[0] == "task_2"

        # Delete non-existent item returns False
        assert pq.delete("task_1") is False

    def test_clear(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_1", priority=10)
        pq.insert("task_2", priority=20)
        pq.clear()
        assert pq.is_empty() is True
        assert pq.size() == 0

    def test_peek_invalid_mode(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        with pytest.raises(ValueError, match="mode must be"):
            pq.peek(mode="invalid")

    def test_extract_empty_queue(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        assert pq.extract_min() is None
        assert pq.extract_max() is None
        assert pq.peek() is None

    def test_get_all_items(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("a", priority=30)
        pq.insert("b", priority=10)
        pq.insert("c", priority=20)

        items = pq.get_all_items()
        assert len(items) == 3
        # Sorted by effective priority ascending
        ids = [i["item_id"] for i in items]
        assert ids[0] == "b"

        for item in items:
            assert "item_id" in item
            assert "priority" in item
            assert "effective_priority" in item
            assert "payload" in item
            assert "inserted_at" in item
            assert "age_seconds" in item


# ---------------------------------------------------------------------------
# 2. Persistence Recovery Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend_type", ["sqlite", "json"])
class TestPersistenceRecovery:

    def test_persistence_across_restart(self, tmp_path, backend_type):
        ext = "db" if backend_type == "sqlite" else "json"
        storage_path = str(tmp_path / f"persist_test.{ext}")

        # Step 1: Create queue, insert items, close
        pq1 = PersistentPriorityQueue(backend=backend_type, storage_file=storage_path)
        pq1.insert("job_a", priority=30, payload={"type": "email"})
        pq1.insert("job_b", priority=5, payload={"type": "sms"})
        pq1.close()

        # Step 2: Simulate restart - new instance, same file
        pq2 = PersistentPriorityQueue(backend=backend_type, storage_file=storage_path)
        assert pq2.size() == 2
        assert pq2.is_empty() is False

        min_item = pq2.extract_min()
        assert min_item == ("job_b", 5.0, {"type": "sms"})

        max_item = pq2.extract_max()
        assert max_item == ("job_a", 30.0, {"type": "email"})

        assert pq2.is_empty() is True
        pq2.close()

    def test_persistence_after_update_and_delete(self, tmp_path, backend_type):
        ext = "db" if backend_type == "sqlite" else "json"
        storage_path = str(tmp_path / f"persist_ud_test.{ext}")

        pq1 = PersistentPriorityQueue(backend=backend_type, storage_file=storage_path)
        pq1.insert("t1", priority=10, payload="original")
        pq1.insert("t2", priority=20, payload="keep")
        pq1.insert("t3", priority=30, payload="remove")
        pq1.update("t1", priority=5, payload="updated")
        pq1.delete("t3")
        pq1.close()

        pq2 = PersistentPriorityQueue(backend=backend_type, storage_file=storage_path)
        assert pq2.size() == 2
        min_item = pq2.extract_min()
        assert min_item == ("t1", 5.0, "updated")
        assert pq2.delete("t2") is True
        assert pq2.is_empty() is True
        pq2.close()


# ---------------------------------------------------------------------------
# 3. Priority Aging / Anti-Starvation Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fixture_name", ["sqlite_queue_with_decay", "json_queue_with_decay"])
class TestPriorityAging:

    def test_old_task_overtakes_newer_task(self, request, fixture_name):
        """
        With decay_rate=50.0/sec:
          Old task: base=100. Age ~0.15s -> effective = 100 - (50 * 0.15) ≈ 92.5
          New task: base=98. Age ~0.00s  -> effective = 98 - 0 ≈ 98.0
        The old task should overtake the newer task and be extracted first.
        """
        pq = request.getfixturevalue(fixture_name)
        pq.insert("old_low_priority", priority=100, payload="old")
        time.sleep(0.15)
        pq.insert("new_higher_priority", priority=98, payload="new")

        extracted = pq.extract_min()
        assert extracted is not None
        assert extracted[0] == "old_low_priority"

    def test_get_all_items_shows_effective_priority(self, request, fixture_name):
        pq = request.getfixturevalue(fixture_name)
        pq.insert("task_1", priority=50, payload="data")
        time.sleep(0.05)

        items = pq.get_all_items()
        assert len(items) == 1
        item = items[0]
        assert item["effective_priority"] < item["priority"]
        assert item["age_seconds"] >= 0.0

    def test_compute_effective_priority_static(self, request, fixture_name):
        now = time.time()
        assert PersistentPriorityQueue.compute_effective_priority(100, now, 0.0) == 100.0
        eff = PersistentPriorityQueue.compute_effective_priority(100, now - 10, 0.5)
        assert abs(eff - 95.0) < 0.5


class TestDecayRateZero:

    def test_decay_rate_zero_is_standard_queue(self, tmp_path):
        json_file = str(tmp_path / "no_decay.json")
        pq = PersistentPriorityQueue(backend="json", storage_file=json_file, decay_rate=0.0)

        pq.insert("a", priority=10)
        pq.insert("b", priority=5)
        pq.insert("c", priority=20)

        items = pq.get_all_items()
        for item in items:
            assert item["effective_priority"] == item["priority"]

        assert pq.extract_min() == ("b", 5.0, None)
        assert pq.extract_max() == ("c", 20.0, None)
        pq.close()


# ---------------------------------------------------------------------------
# 4. Top-Level Module Helpers & Aliases Tests
# ---------------------------------------------------------------------------

class TestModuleHelpersAndAlias:

    def test_priority_queue_alias(self):
        assert PriorityQueue is PersistentPriorityQueue

    def test_module_level_helpers(self, tmp_path):
        test_file = str(tmp_path / "helper_test.json")
        # Reset default queue
        module._default_instance = None
        module.get_default_queue(backend="json", storage_file=test_file)

        module.insert("m1", priority=30, payload="data1")
        module.insert("m2", priority=10, payload="data2")
        assert module.is_empty() is False

        assert module.peek(mode="min")[0] == "m2"
        assert module.update("m1", priority=5) is True
        assert module.extract_min()[0] == "m1"
        assert module.delete("m2") is True
        assert module.is_empty() is True


# ---------------------------------------------------------------------------
# 5. Edge Cases & Boundary Conditions
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_insert_with_none_payload(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "test.json"))
        pq.insert("no_payload", priority=1)
        result = pq.extract_min()
        assert result == ("no_payload", 1.0, None)
        pq.close()

    def test_insert_with_complex_payload(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "test.json"))
        payload = {"nested": {"list": [1, 2, 3], "bool": True}, "text": "hello"}
        pq.insert("complex", priority=1, payload=payload)
        result = pq.extract_min()
        assert result[2] == payload
        pq.close()

    def test_update_nothing(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "test.json"))
        pq.insert("x", priority=10)
        assert pq.update("x") is False
        pq.close()

    def test_large_queue(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "large.json"))
        for i in range(100):
            pq.insert(f"item_{i:03d}", priority=100 - i)
        assert pq.size() == 100

        result = pq.extract_min()
        assert result[0] == "item_099"
        assert result[1] == 1.0

        result = pq.extract_max()
        assert result[0] == "item_000"
        assert result[1] == 100.0
        pq.close()

    def test_negative_priorities(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "neg.json"))
        pq.insert("neg", priority=-10)
        pq.insert("pos", priority=10)
        pq.insert("zero", priority=0)

        assert pq.extract_min() == ("neg", -10.0, None)
        assert pq.extract_min() == ("zero", 0.0, None)
        assert pq.extract_min() == ("pos", 10.0, None)
        pq.close()

    def test_float_priorities(self, tmp_path):
        pq = PersistentPriorityQueue(backend="json", storage_file=str(tmp_path / "float.json"))
        pq.insert("a", priority=1.5)
        pq.insert("b", priority=1.1)
        pq.insert("c", priority=1.9)

        assert pq.extract_min()[0] == "b"
        assert pq.extract_min()[0] == "a"
        assert pq.extract_min()[0] == "c"
        pq.close()


# ---------------------------------------------------------------------------
# 6. Flask REST API Integration Tests
# ---------------------------------------------------------------------------

class TestFlaskAPI:

    def test_health_endpoint(self, api_client):
        res = api_client.get("/api/health")
        assert res.status_code == 200
        data = res.get_json()
        assert data["status"] == "healthy"
        assert "queue_size" in data

    def test_stats_endpoint(self, api_client):
        res = api_client.get("/api/stats")
        assert res.status_code == 200
        data = res.get_json()
        assert "size" in data
        assert "is_empty" in data
        assert "decay_rate" in data
        assert "extracted_count" in data

    def test_insert_and_get_queue(self, api_client):
        # Insert item
        res = api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "api_t1", "priority": 25, "payload": "test payload"}),
            content_type="application/json",
        )
        assert res.status_code == 201
        assert res.get_json()["success"] is True

        # Fetch queue
        res = api_client.get("/api/queue")
        assert res.status_code == 200
        data = res.get_json()
        assert len(data["items"]) == 1
        assert data["items"][0]["item_id"] == "api_t1"

    def test_extract_min_and_max(self, api_client):
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "low_prio", "priority": 10}),
            content_type="application/json",
        )
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "high_prio", "priority": 90}),
            content_type="application/json",
        )

        # Extract min
        res_min = api_client.post("/api/extract-min")
        assert res_min.status_code == 200
        assert res_min.get_json()["item"]["item_id"] == "low_prio"

        # Extract max
        res_max = api_client.post("/api/extract-max")
        assert res_max.status_code == 200
        assert res_max.get_json()["item"]["item_id"] == "high_prio"

        # Empty extraction returns 404
        res_empty = api_client.post("/api/extract-min")
        assert res_empty.status_code == 404

    def test_peek_endpoint(self, api_client):
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "p1", "priority": 15}),
            content_type="application/json",
        )
        res = api_client.get("/api/peek?mode=min")
        assert res.status_code == 200
        assert res.get_json()["item"]["item_id"] == "p1"

    def test_update_endpoint_restful(self, api_client):
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "u1", "priority": 50}),
            content_type="application/json",
        )
        # PUT /api/update/<item_id>
        res = api_client.put(
            "/api/update/u1",
            data=json.dumps({"priority": 5, "payload": "updated"}),
            content_type="application/json",
        )
        assert res.status_code == 200
        assert res.get_json()["success"] is True

        # Verify update took effect
        peek_res = api_client.get("/api/peek")
        assert peek_res.get_json()["item"]["priority"] == 5.0

    def test_delete_endpoint_restful(self, api_client):
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "d1", "priority": 30}),
            content_type="application/json",
        )
        # DELETE /api/delete/<item_id>
        res = api_client.delete("/api/delete/d1")
        assert res.status_code == 200
        assert res.get_json()["success"] is True

        # Non-existent delete returns 404
        res404 = api_client.delete("/api/delete/d1")
        assert res404.status_code == 404

    def test_clear_endpoint(self, api_client):
        api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "c1", "priority": 1}),
            content_type="application/json",
        )
        res = api_client.post("/api/clear")
        assert res.status_code == 200

        stats_res = api_client.get("/api/stats")
        assert stats_res.get_json()["size"] == 0

    def test_input_validation_errors(self, api_client):
        # Missing priority
        r1 = api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "err1"}),
            content_type="application/json",
        )
        assert r1.status_code == 400

        # Missing item_id
        r2 = api_client.post(
            "/api/insert",
            data=json.dumps({"priority": 10}),
            content_type="application/json",
        )
        assert r2.status_code == 400

        # Non-numeric priority
        r3 = api_client.post(
            "/api/insert",
            data=json.dumps({"item_id": "err3", "priority": "not_a_number"}),
            content_type="application/json",
        )
        assert r3.status_code == 400

        # Non-json body
        r4 = api_client.post("/api/insert", data="plain text")
        assert r4.status_code == 400

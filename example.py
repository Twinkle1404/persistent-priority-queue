"""
example.py - Demonstration of Persistent Priority Queue with Priority Aging

This script showcases:
  1. Standard Priority Queue operations (insert, extract_min, peek, update, delete)
  2. Priority Aging / Anti-Starvation in action
  3. Crash Durability & State Persistence across process restarts
  4. Launch instructions for the Live Observability Web Dashboard
"""

import time
from module import PersistentPriorityQueue


def main():
    json_file = "example_queue.json"

    print("=" * 68)
    print("  [*] Persistent Priority Queue -- SDE Demonstration & Walkthrough")
    print("=" * 68)

    # ------------------------------------------------------------------
    # 1. Basic Queue Operations (decay_rate = 0.0)
    # ------------------------------------------------------------------
    print("\n--- 1. Basic Operations (File-Based JSON Persistence) ---\n")

    pq = PersistentPriorityQueue(backend="json", storage_file=json_file, decay_rate=0.0)
    pq.clear()

    tasks = [
        ("task_analytics",   100, "Generate monthly financial report"),
        ("task_payment_500",   1, "Fix payment gateway 500 error"),
        ("task_ui_darkmode",  50, "Add dark mode toggle to dashboard"),
        ("task_cve_patch",     5, "Apply critical kernel security patch"),
    ]

    for item_id, priority, payload in tasks:
        pq.insert(item_id=item_id, priority=priority, payload=payload)
        print(f"  [+] Inserted: {item_id:<18} | Priority: {priority:>3} | Payload: {payload}")

    print(f"\n  Current Queue Size: {pq.size()}")
    print(f"  Peek Min (Urgent):  {pq.peek_min()}")
    print(f"  Peek Max (Lowest):  {pq.peek_max()}")

    print("\n  Updating 'task_analytics' from priority 100 -> 2 (Urgent Escalation)...")
    pq.update("task_analytics", priority=2)
    print(f"  New Peek Min:       {pq.peek_min()}")

    print("\n  Extracting all tasks in priority order (lowest priority score = highest urgency):")
    while not pq.is_empty():
        item_id, priority, payload = pq.extract_min()
        print(f"    -> Extracted: {item_id:<18} | Priority: {priority:<4} | {payload}")

    pq.close()

    # ------------------------------------------------------------------
    # 2. Priority Aging / Anti-Starvation Mechanism
    # ------------------------------------------------------------------
    print("\n--- 2. Priority Aging & Anti-Starvation Demo (decay_rate = 20.0/sec) ---\n")
    print("  Problem: In standard priority queues, low-priority tasks starve when new urgent tasks arrive.")
    print("  Solution: Effective Priority = Base Priority - (Decay Rate * Age Seconds).\n")

    pq_aging = PersistentPriorityQueue(backend="json", storage_file=json_file, decay_rate=20.0)
    pq_aging.clear()

    # Insert old low-urgency task
    pq_aging.insert("task_old_batch", priority=100, payload="Nightly database backup")
    print("  [t=0s] Inserted 'task_old_batch' (Base Priority: 100)")

    # Simulate passage of time
    print("  Waiting 2.0 seconds for aging decay...")
    time.sleep(2.0)

    # Insert a newer, nominally higher-priority task
    pq_aging.insert("task_new_alert", priority=75, payload="Disk space warning (90%)")
    print("  [t=2s] Inserted 'task_new_alert' (Base Priority: 75)")

    print("\n  Queue State after Aging:")
    for item in pq_aging.get_all_items():
        print(
            f"    - {item['item_id']:<16} | Base: {item['priority']:>5} | "
            f"Effective: {item['effective_priority']:>6.2f} | Age: {item['age_seconds']:.1f}s"
        )

    # The older task has decayed from 100 -> ~60, which is < 75. It will be extracted first!
    extracted = pq_aging.extract_min()
    print(f"\n  -> extract_min() returned: '{extracted[0]}' (Base Priority: {extracted[1]:.2f})")
    print("  [OK] Proven: Older task overtook newer task due to aging decay, preventing starvation!")
    pq_aging.close()

    # ------------------------------------------------------------------
    # 3. Crash Resilience & Persistence Recovery
    # ------------------------------------------------------------------
    print("\n--- 3. Crash Durability & State Persistence Across Restarts ---\n")

    pq_persist = PersistentPriorityQueue(backend="json", storage_file=json_file)
    pq_persist.clear()
    pq_persist.insert("durable_task_1", priority=10, payload={"action": "reboot_cluster"})
    pq_persist.insert("durable_task_2", priority=40, payload={"action": "sync_s3_bucket"})
    print(f"  Inserted 2 tasks to disk file '{json_file}'.")
    print("  Simulating immediate process kill (closing Python instance)...")
    pq_persist.close()

    # Simulate app restart
    pq_recovered = PersistentPriorityQueue(backend="json", storage_file=json_file)
    print(f"  Reopened queue from disk. Recovered size: {pq_recovered.size()}")
    item1 = pq_recovered.extract_min()
    item2 = pq_recovered.extract_min()
    print(f"  Extracted from disk: {item1[0]} (Priority: {item1[1]})")
    print(f"  Extracted from disk: {item2[0]} (Priority: {item2[1]})")
    print(f"  Queue is now empty: {pq_recovered.is_empty()}")
    pq_recovered.close()

    # ------------------------------------------------------------------
    # 4. Live Dashboard Launch Instructions
    # ------------------------------------------------------------------
    print("\n--- 4. Live Observability Dashboard ---\n")
    print("  To launch the real-time web monitoring console:")
    print("    $ python server.py")
    print("    Then visit: http://localhost:5000 in your browser.")
    print("=" * 68 + "\n")


if __name__ == "__main__":
    main()

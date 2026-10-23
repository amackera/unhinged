"""Port of Absurd's quickstart client.

    python examples/quickstart_client.py alice            # spawn
    python examples/quickstart_client.py alice --await    # spawn and wait
    python -m unhinged --db quickstart.db emit user-activated:alice '{"activated_at": "now"}'
"""

import sys

from unhinged import Unhinged

should_await = "--await" in sys.argv
args = [a for a in sys.argv[1:] if a != "--await"]
user_id = args[0] if args else "alice"
email = args[1] if len(args) > 1 else f"{user_id}@example.com"

app = Unhinged("quickstart.db", queue_name="default")
spawned = app.spawn("provision-user", {"user_id": user_id, "email": email}, queue="default")
print("spawned:", spawned)
print("current snapshot:", app.fetch_task_result(spawned["task_id"]))

if should_await:
    print(f"waiting; emit user-activated:{user_id} on queue default to finish")
    print("final snapshot:", app.await_task_result(spawned["task_id"], timeout=300))

app.close()

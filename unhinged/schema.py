"""DDL for the Unhinged tables.

This is a straight port of the table layout in Absurd's ``sql/absurd.sql``.
Absurd puts everything in a Postgres schema called ``absurd`` and materialises
one set of tables per queue, sharing a prefix:

* ``t_<queue>``  tasks        (what is to be run)
* ``r_<queue>``  runs         (attempts to run a task)
* ``c_<queue>``  checkpoints  (saved step results)
* ``e_<queue>``  events       (emitted signals, first write wins)
* ``w_<queue>``  waits        (runs suspended on an event)

SQLite has no schemas, so the tables live at the top level of the database
file, and no stored procedures, so every ``absurd.*`` function lives in
:mod:`unhinged.engine` as a Python method that runs inside one short
``BEGIN IMMEDIATE`` transaction.

Type mapping:

* ``uuid``        -> TEXT (UUIDv7, lowercase hyphenated)
* ``jsonb``       -> TEXT holding JSON (SQL NULL means "absent", the text
                     ``null`` means JSON null, exactly like jsonb)
* ``timestamptz`` -> TEXT, ISO-8601 in UTC, fixed width
                     ``YYYY-MM-DDTHH:MM:SS.ffffff`` so lexical order is
                     chronological order
* ``interval``    -> INTEGER seconds

Dropped on purpose (Postgres-only): partitioned storage mode, the ``i_``
idempotency side table that only partitioned queues need, and the pg_cron
scheduling helpers.
"""

from __future__ import annotations

SCHEMA_VERSION = "0.5.1-sqlite"

STATES = ("pending", "running", "sleeping", "completed", "failed", "cancelled")
_STATE_CHECK = "check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled'))"

# Mirrors absurd.queues minus the partition policy columns.
QUEUES_DDL = """
create table if not exists queues (
  queue_name    text primary key,
  created_at    text not null,
  cleanup_ttl   integer not null default 2592000 check (cleanup_ttl >= 0),
  cleanup_limit integer not null default 1000 check (cleanup_limit >= 1)
);
"""


def ident(name: str) -> str:
    """Quote an identifier the way Postgres' %I does."""
    return '"' + name.replace('"', '""') + '"'


def table(prefix: str, queue: str) -> str:
    return ident(f"{prefix}_{queue}")


def queue_ddl(queue: str) -> list[str]:
    """Return the statements that create one queue's tables and indexes.

    Mirrors ``absurd.ensure_queue_tables`` for an unpartitioned queue.
    """
    t, r, c, e, w = (table(p, queue) for p in "trcew")
    return [
        f"""
        create table if not exists {t} (
          task_id           text primary key,
          task_name         text not null,
          params            text not null,
          headers           text,
          retry_strategy    text,
          max_attempts      integer,
          cancellation      text,
          enqueue_at        text not null,
          first_started_at  text,
          state             text not null {_STATE_CHECK},
          attempts          integer not null default 0,
          last_attempt_run  text,
          completed_payload text,
          cancelled_at      text,
          idempotency_key   text unique
        )
        """,
        f"""
        create table if not exists {r} (
          run_id           text primary key,
          task_id          text not null,
          attempt          integer not null,
          state            text not null {_STATE_CHECK},
          claimed_by       text,
          claim_expires_at text,
          available_at     text not null,
          wake_event       text,
          event_payload    text,
          started_at       text,
          completed_at     text,
          failed_at        text,
          result           text,
          failure_reason   text,
          created_at       text not null
        )
        """,
        f"""
        create table if not exists {c} (
          task_id         text not null,
          checkpoint_name text not null,
          state           text,
          status          text not null default 'committed',
          owner_run_id    text,
          updated_at      text not null,
          primary key (task_id, checkpoint_name)
        )
        """,
        f"""
        create table if not exists {e} (
          event_name text primary key,
          payload    text,
          emitted_at text not null
        )
        """,
        f"""
        create table if not exists {w} (
          task_id    text not null,
          run_id     text not null,
          step_name  text not null,
          event_name text not null,
          timeout_at text,
          created_at text not null,
          primary key (run_id, step_name)
        )
        """,
        f"create index if not exists {ident(f'r_{queue}_sai')} on {r} (state, available_at)",
        f"create index if not exists {ident(f'r_{queue}_ti')} on {r} (task_id)",
        f"create index if not exists {ident(f'r_{queue}_cei')} on {r} (claim_expires_at) "
        f"where state = 'running' and claim_expires_at is not null",
        f"create index if not exists {ident(f't_{queue}_ci')} on {t} (task_id) "
        f"where cancellation is not null",
        f"create index if not exists {ident(f'w_{queue}_eni')} on {w} (event_name)",
        f"create index if not exists {ident(f'w_{queue}_ti')} on {w} (task_id)",
        f"create index if not exists {ident(f'e_{queue}_eai')} on {e} (emitted_at)",
    ]


def queue_drop_ddl(queue: str) -> list[str]:
    return [f"drop table if exists {table(p, queue)}" for p in "wecrt"]

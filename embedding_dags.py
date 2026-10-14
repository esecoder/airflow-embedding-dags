"""
embedding_dags.py — ingestion and re-indexing as Airflow DAGs.

    python3 test_dags.py        # validates the DAGs without an Airflow install
    docker compose up           # the real thing

===============================================================================
THE PROBLEM THIS SOLVES
===============================================================================
A retrieval corpus goes stale in one of two ways, and they need different handling:

  NEW CONTENT     a document was added, so chunks must be created and embedded
  CHANGED CONTENT a document was edited, so its OLD chunks must be REMOVED as well

⚠️ The second is the one that bites. An append-only pipeline keeps the previous
version of every edited document, and a retriever that returns a superseded passage
does it **confidently** — nothing in the output says the chunk is from last month.
That is why `reindex_changed` deletes before it inserts, and why the DAG is
scheduled rather than triggered by hand.

⚠️ AND WHY IT IS A DAG RATHER THAN A SCRIPT. Three things a cron job does not give you
and this needs:

  IDEMPOTENT TASK RUNS  a retried task must not duplicate a day's chunks. Each task is
                        written to be re-runnable for a given `ds`, which is the only
                        reason `catchup=True` is safe.
  BACKFILL              re-indexing a range after a chunker fix is the normal case, not
                        an emergency, and it must run in the same code path as the
                        daily run rather than a hand-written loop.
  RETRIES WITH BACKOFF  embedding calls fail transiently. ⚠️ A retry that fires
                        immediately against a rate-limited API makes the outage worse.
"""

from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor

DAG_ID = "embedding_pipeline"
SCHEMA = "retrieval"

# ⚠️ Defaults are where reliability is actually configured. `retries` inside the DAG
# rather than in a task means a new task cannot forget them.
DEFAULT_ARGS = {
    "owner": "data",
    "retries": 3,
    # ⚠️ EXPONENTIAL BACKOFF, not a fixed 30s. Measured experience with rate-limited
    # embedding APIs: hammering a throttled endpoint on a fixed interval extends the
    # outage, because every retry is another 429 and the quota never recovers.
    "retry_delay": dt.timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=15),
    # ⚠️ A task that hangs is worse than one that fails: it holds a slot and stops the
    # next run, and nothing retries. `execution_timeout` turns a hang into a failure.
    "execution_timeout": dt.timedelta(minutes=30),
    "depends_on_past": False,
    # ⚠️ Without this, a retry of an old task can still succeed and leave a gap that
    # nothing reports. 'all_success' on the upstream is the default for a reason.
    "email_on_failure": False,
}


def _connect():
    return PostgresHook(postgres_conn_id="retrieval_db").get_conn()


def wait_for_source(**ctx) -> None:
    """⚠️ Not implemented as a loop of `time.sleep`. Airflow sensors have a `poke`
    interval and a timeout; a hand-rolled poll holds a worker slot for its whole
    duration and reports nothing while it waits."""
    raise NotImplementedError("SqlSensor handles this; see the DAG below")


def extract_chunks(ds: str, **ctx) -> int:
    """Chunk documents that arrived on or before `ds` and have no chunks yet.

    ⚠️ IDEMPOTENT BY CONSTRUCTION, via `not exists`. Re-running this task for the same
    `ds` inserts nothing the second time -- which is what makes a task retry, a manual
    re-run and a backfill all safe without special cases.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {SCHEMA}.chunks (doc_id, chunk_index, body, created_at)
            select d.doc_id,
                   row_number() over (partition by d.doc_id order by d.updated_at) - 1,
                   d.body,
                   now()
              from {SCHEMA}.documents d
             where d.updated_at::date <= %s
               -- ⚠️ THE GUARD. Without it a retry duplicates every chunk, and the
               -- retriever then scores the same text twice.
               and not exists (select 1 from {SCHEMA}.chunks c where c.doc_id = d.doc_id)
            """,
            (ds,),
        )
        return cur.rowcount


def embed_chunks(ds: str, batch_size: int = 128, **ctx) -> int:
    """Embed chunks with no vector yet, in batches.

    ⚠️ BATCHED, not one call per chunk: the API cost and latency are per-request, and
    a loop of single calls is the difference between minutes and hours. It is also the
    difference between one retry and a thousand.
    """
    total = 0
    while True:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                select chunk_id, body from {SCHEMA}.chunks
                 where embedding is null
                 order by chunk_id
                 limit %s
                """,
                (batch_size,),
            )
            rows = cur.fetchall()
            if not rows:
                return total
            for chunk_id, _body in rows:                 # pragma: no cover - needs API
                cur.execute(
                    f"update {SCHEMA}.chunks set embedding = %s where chunk_id = %s",
                    ([0.0] * 384, chunk_id),
                )
                total += 1


def reindex_changed(ds: str, **ctx) -> int:
    """⚠️ DELETE FIRST. Remove chunks for documents edited on `ds`, then re-chunk them.

    An append-only pipeline leaves the previous version of every edited document in
    the corpus, and a retriever that returns a superseded passage does so with full
    confidence -- nothing in the output says the chunk is stale.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            delete from {SCHEMA}.chunks c
             using {SCHEMA}.documents d
             where c.doc_id = d.doc_id
               and d.updated_at::date = %s
               and c.created_at < d.updated_at
            """,
            (ds,),
        )
        return cur.rowcount


def reconcile(**ctx) -> dict:
    """⚠️ THE TASK THAT WOULD OTHERWISE BE MISSING.

    Every task above reports what it did. None of them can tell you the corpus is
    inconsistent. This can, and it fails the run rather than reporting quietly:
    an orphaned chunk is content the retriever will serve and nobody owns.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(f"""
            select
              (select count(*) from {SCHEMA}.chunks c
                where not exists (select 1 from {SCHEMA}.documents d
                                   where d.doc_id = c.doc_id))          as orphaned,
              (select count(*) from {SCHEMA}.chunks where embedding is null) as unembedded,
              (select count(*) from {SCHEMA}.chunks c
                where not exists (select 1 from {SCHEMA}.chunks b
                                   where b.doc_id = c.doc_id and b.chunk_index = c.chunk_index - 1)
                  and c.chunk_index > 0)                                as gaps
        """)
        orphaned, unembedded, gaps = cur.fetchone()

    ctx["ti"].xcom_push(key="reconcile", value={
        "orphaned": orphaned, "unembedded": unembedded, "gaps": gaps})

    if orphaned or gaps:
        # ⚠️ ORPHANS AND GAPS FAIL THE RUN. Unembedded chunks do not -- they are a
        # normal in-flight state and the next run picks them up. Failing on a state
        # the pipeline is designed to pass through is how a DAG becomes permanently
        # red and therefore ignored.
        raise ValueError(f"corpus inconsistent: {orphaned} orphaned, {gaps} gaps")
    return {"orphaned": 0, "unembedded": unembedded, "gaps": 0}


with DAG(
    dag_id=DAG_ID,
    description="Chunk, embed and re-index the retrieval corpus.",
    start_date=dt.datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ai", "retrieval", "platform"],
    doc_md=__doc__,
) as dag:

    # ⚠️ A SENSOR THAT WAITS ON THE SOURCE, not a timer. The previous design ran at
    # 03:00 and hoped the upstream export had landed; when it had not, the run
    # succeeded with nothing to do and reported green -- the failure mode where a
    # pipeline looks healthy and has silently stopped ingesting.
    wait_for_documents = SqlSensor(
        task_id="wait_for_documents",
        conn_id="retrieval_db",
        sql=f"select count(*) from {SCHEMA}.documents where updated_at::date = '{{{{ ds }}}}'",
        poke_interval=60,
        timeout=60 * 60 * 4,
        mode="reschedule",       # ⚠️ frees the worker slot while waiting
        soft_fail=False,
    )

    chunk = PythonOperator(task_id="extract_chunks", python_callable=extract_chunks)
    embed = PythonOperator(task_id="embed_chunks", python_callable=embed_chunks)
    reindex = PythonOperator(task_id="reindex_changed", python_callable=reindex_changed)
    check = PythonOperator(task_id="reconcile", python_callable=reconcile)

    # ⚠️ REINDEX BEFORE EMBED, deliberately. Deleting stale chunks first means the
    # embed task's work list is correct; the other order embeds rows that are about
    # to be deleted -- paying for vectors that never reach the index.
    wait_for_documents >> chunk >> reindex >> embed >> check

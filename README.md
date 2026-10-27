# airflow-embedding-dags

**Keeping a retrieval corpus current: chunk, re-index and embed, as a scheduled DAG.**

```bash
python3 test_dags.py        # 21 structural checks, no Airflow install
docker compose up           # the real thing → localhost:8080
```

## The problem it solves

 **A corpus goes stale in two ways, and they are not the same problem.**

| | |
|---|---|
| **New content** | a document was added → chunks must be created and embedded |
| **Changed content** | a document was **edited** → its *old* chunks must be **removed too** |

 **The second is the one that bites.** An append-only pipeline keeps the previous version of every edited document, and **a retriever that returns a superseded passage does it confidently** — nothing in the output says the chunk is from last month. That is why `reindex_changed` deletes before it inserts.

## Why a DAG and not a cron script

**Idempotent task runs.** Every task is written to be re-runnable for a given `ds`, which is the only reason `catchup=True` is safe. `extract_chunks` guards with `not exists` — without it a retried task duplicates every chunk and the retriever scores the same text twice.

**Backfill as a normal case.** Re-indexing a date range after a chunker fix should run the same code path as the daily run, not a hand-written loop.

**Retries with backoff.**  Embedding calls fail transiently, and **a retry that fires immediately against a rate-limited API makes the outage worse** — every retry is another 429 and the quota never recovers. `retry_exponential_backoff` with a 15-minute cap.

## The four decisions worth defending

**1. `execution_timeout` on every task.**  **A task that hangs is worse than one that fails** — it holds a worker slot, stops the next run, and nothing retries. A timeout turns a hang into a failure.

**2. Reindex runs *before* embed.** Deleting stale chunks first means the embed task's work list is correct. The other order **embeds rows that are about to be deleted — paying for vectors that never reach the index.**

**3. The sensor waits on data, not a clock.** The previous design ran at 03:00 and hoped the upstream export had landed; when it had not, the run **succeeded with nothing to do and reported green** — the failure mode where a pipeline looks healthy and has silently stopped ingesting. `mode="reschedule"` also frees the worker slot while waiting.

**4. Unembedded chunks do *not* fail the run; orphans and gaps do.**

 **That distinction is the important one.** Unembedded is an in-flight state the next run picks up — failing on it makes the DAG **permanently red and therefore ignored.** An orphaned chunk is different: it is content the retriever will serve **and nobody owns.**

##  What the tests do and do not prove

`test_dags.py` parses the source with `ast` and asserts on structure: the retry policy, the backoff, the timeout, the task order, the idempotence guards, the failure taxonomy.

 **It is weaker than running it, and says so.** It cannot catch a malformed operator argument, a bad connection id, or a SQL dialect error. **`docker compose up` is what proves those**, which is why the compose file is here rather than described.

 **Airflow is not installed on this machine** — it needs a scheduler, a metadata database and a web server. The structural test runs anywhere with `python3`; the compose file is the honest next step, not a claim that has been made.

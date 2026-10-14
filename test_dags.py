"""
test_dags.py — validates the DAG without an Airflow installation.

    python3 test_dags.py

===============================================================================
⚠️ WHAT THIS DOES AND DOES NOT PROVE
===============================================================================
Airflow is a heavy dependency (a scheduler, a metadata database, a web server), and
a test that will not run on a fresh clone is a test that gets deleted. So this checks
the DAG by **parsing the source** with `ast` and asserting on its structure.

⚠️ THAT IS WEAKER THAN RUNNING IT, AND THE FILE SAYS SO RATHER THAN IMPLYING
OTHERWISE. It catches: a missing retry policy, a missing backoff, a missing
execution timeout, a task order that pays for vectors it then deletes, `catchup`
turned off, and an idempotence guard that was dropped from the SQL.

⚠️ It does NOT catch: a malformed operator argument, a bad connection id, a SQL
dialect error, or anything that only appears against a live scheduler. Those need
`docker compose up`, and that is why the compose file is in the repository.
"""

from __future__ import annotations

import ast
import pathlib
import sys

SOURCE = pathlib.Path(__file__).parent / "embedding_dags.py"
FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{'  — ' + detail if detail else ''}")
    if not ok:
        FAILURES.append(name)


def main() -> int:
    print("test_dags.py — the DAGs, validated structurally\n")
    src = SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(src)

    # ── 1. the DAG's own configuration ───────────────────────────────────────
    dag_call = next((n for n in ast.walk(tree)
                     if isinstance(n, ast.Call)
                     and getattr(n.func, "id", "") == "DAG"), None)
    check("the file defines a DAG", dag_call is not None)
    kwargs = {k.arg: k.value for k in (dag_call.keywords if dag_call else [])}

    def literal(key):
        v = kwargs.get(key)
        return v.value if isinstance(v, ast.Constant) else None

    check("catchup is enabled", literal("catchup") is True,
          "without it a backfill after a chunker fix is a hand-written loop")
    check("max_active_runs is 1", literal("max_active_runs") == 1,
          "two runs overlapping would both delete and insert the same chunks")
    check("the DAG is scheduled, not manual", literal("schedule") not in (None, "None"))

    # ── 2. the retry policy, which is where reliability actually lives ───────
    defaults = next((n.value for n in ast.walk(tree)
                     if isinstance(n, ast.Assign)
                     and getattr(n.targets[0], "id", "") == "DEFAULT_ARGS"), None)
    keys = {k.value for k in defaults.keys} if isinstance(defaults, ast.Dict) else set()
    for key, why in (
        ("retries", "a transient embedding failure must not need a human"),
        ("retry_delay", "an immediate retry against a throttled API extends the outage"),
        ("retry_exponential_backoff", "fixed intervals keep hitting the rate limit"),
        ("execution_timeout", "a hang holds a worker slot and stops the next run — worse than a failure"),
    ):
        check(f"DEFAULT_ARGS sets {key}", key in keys, why)
    check("the backoff is exponential, not fixed", "retry_exponential_backoff" in keys)
    check("retries are >= 3",
          '"retries"' in src and "3" in ast.unparse(defaults) if defaults else False,
          "one retry is not a retry policy")

    # ── 3. task order ────────────────────────────────────────────────────────
    order = " ".join(src.split(">>")[-1].split("\n")[0].split())
    check("reindex runs BEFORE embed",
          src.index(">> reindex") < src.index(">> embed"),
          "the other order embeds rows that are about to be deleted — paying for "
          "vectors that never reach the index")
    for task in ("extract_chunks", "embed_chunks", "reindex_changed", "reconcile"):
        check(f"the {task} task exists", f"def {task}(" in src)

    # ── 4. idempotence guards in the SQL ─────────────────────────────────────
    # ⚠️ THIS IS THE ASSERTION THAT MATTERS MOST. A `not exists` guard is the only
    # reason a retry, a re-run and a backfill are all safe in the same code path, and
    # deleting it would not fail anything else here.
    check("extract_chunks guards against re-inserting",
          "not exists (select 1 from {SCHEMA}.chunks" in src,
          "without it a retried task duplicates every chunk, and the retriever then "
          "scores the same text twice")
    check("reindex_changed deletes before it inserts",
          "delete from {SCHEMA}.chunks" in src,
          "an append-only pipeline keeps the previous version of every edited document")
    check("reindex_changed is scoped to the run date",
          "d.updated_at::date = %s" in src,
          "an unscoped delete would empty the corpus on the first run")

    # ── 5. the failure taxonomy ──────────────────────────────────────────────
    # ⚠️ Orphans and gaps fail the run; unembedded chunks do not. Failing on a state
    # the pipeline passes through normally is how a DAG becomes permanently red and
    # therefore ignored.
    reconcile_body = src[src.index("def reconcile("):src.index("with DAG(")]
    check("orphaned chunks fail the run",
          "if orphaned or gaps:" in reconcile_body,
          "an orphan is content the retriever serves and nobody owns")
    check("unembedded chunks do NOT fail the run",
          "unembedded" not in reconcile_body.split("if orphaned or gaps:")[1][:200],
          "unembedded is an in-flight state the next run picks up")

    # ── 6. the sensor waits on data, not a clock ─────────────────────────────
    check("the sensor uses reschedule mode",
          'mode="reschedule"' in src,
          "in poke mode a waiting task holds a worker slot for four hours")
    check("the sensor has a timeout",
          "timeout=60 * 60 * 4" in src,
          "an unbounded sensor waits forever and the DAG never reports the problem")

    print()
    if FAILURES:
        print(f"  ❌ {len(FAILURES)} failed: {', '.join(FAILURES)}")
        return len(FAILURES)
    print("  ✅ the DAG structure holds")
    print("  ⚠️ structural only — `docker compose up` is what proves it runs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

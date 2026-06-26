# Scaling m-cube Backtests: From In-Process Runs to a Global Task Queue

> **What this document is.** A plain-language explanation of *why* m-cube cannot currently
> run many backtests in parallel, *what* in the runtime causes that limit, and *how* a
> global task queue (Celery) fixes it. It is written so that someone seeing the project for
> the first time can follow the argument end-to-end. No application code has been changed —
> everything below is design and rationale.

---

## TL;DR

m-cube runs trading-strategy **backtests** on top of [NautilusTrader](https://nautilustrader.io)
(`nautilus_trader==1.224.0`). Today, each backtest runs *inside the web server's own process*,
and the heavy compute step (`engine.run()`) holds the Python **GIL** for its entire duration.
Because of that, you cannot get real parallelism by adding threads — the runs just take turns.

The good news: a single backtest is a completely self-contained, throwaway unit of work that
needs no database, no shared state, and no coordination with other runs. That makes backtests
**embarrassingly parallel** — the only thing missing is a way to hand each run to its own
process on its own core (or its own machine).

The fix is to take every `engine.run()` out of the web request and push it onto a **global
task queue** (Celery), where a pool of dedicated worker *processes* — each with its own GIL —
pulls tasks and runs them in true parallel. The web tier then only validates requests and
enqueues jobs.

---

## 1. Background: how one backtest runs today

### 1.1 One portfolio → one process

Right now a single portfolio runs in a single process. In other words, **each portfolio
backtest has a dedicated OS process doing the actual computation**. That process builds a
NautilusTrader `BacktestEngine`, feeds it historical market data, and lets it run to
completion.

This one-process-per-run boundary is not an accident — it is the natural isolation unit for
NautilusTrader. A `BacktestEngine` constructs its own `Cache`, `MessageBus`, `Portfolio`,
simulated exchange, and clock *inside itself*, and several of these behave as
process-scoped/singleton-style components. (This is the subject of the internal
*"Global Singleton Architecture in NautilusTrader v1.224.0"* note — see
[§9, Sources](#9-sources-and-further-reading).) Because that state lives at the process level,
the clean, collision-free boundary between two independent runs is the **OS process**: give each
`engine.run()` its own process and nothing leaks between them.

### 1.2 `engine.run()` is CPU-bound and holds the GIL

When we call `engine.run()` on a `BacktestEngine`, it is a **CPU-bound** operation. It does not
wait on the network or a database; it just grinds through the data stream as fast as the CPU
allows. For the entire time it is running, it **holds the Global Interpreter Lock (GIL)**.

At the implementation level, `engine.run()` behaves like a tight
`while <data remaining>: process_next_event()` loop in compiled (Cython/Rust) code that keeps
executing — and keeps the GIL held — until the data stream is exhausted and the run ends.

> **What is the GIL?** CPython's *Global Interpreter Lock* is a mutex that allows **only one
> thread per process to execute Python/Cython bytecode at any instant**. It is fine for
> I/O-bound work (a thread releases the GIL while waiting on the network), but for a CPU-bound
> loop like `engine.run()` that never blocks on I/O, the lock is held continuously. Two such
> loops in two threads of the *same* process therefore cannot run at the same time — they are
> serialized by the GIL.

The consequence is the single most important fact in this whole document:

> **You cannot parallelize CPU-bound backtests with threads. Real parallelism requires
> separate processes**, because each process has its own GIL.

### 1.3 Today the runs are tied to Flask's threads

The backtests are currently launched from within **Flask's request/background threads** (the
app runs on the Werkzeug dev server with `threaded=True`). Because the run lives inside the web
process, three problems follow:

1. **No real parallelism across runs.** Threads in one process share one GIL, and
   `engine.run()` holds it the whole time, so concurrent backtests effectively run one after
   another instead of side by side.
2. **CPU is coupled to the web tier.** A single long backtest ties up a web replica that should
   be free to serve other users' HTTP requests.
3. **Jobs are fragile.** A run's lifecycle is bound to its HTTP request and its web process —
   if the request times out or the container restarts, the job dies with it.

This is the bottleneck that blocks scaling.

---

## 2. Why backtests are *actually easy* to scale

Before the fix, it is worth stating clearly why this is a tractable problem. Containerizing
and parallelizing m-cube is **not** a NautilusTrader problem — it is a web-app plumbing
problem. From the Nautilus concept docs:

- **The engine is in-process and ephemeral.** A backtest is `BacktestEngine()` →
  `add_venue / add_instrument / add_data / add_strategy` → `run()`. When the data stream is
  exhausted, it produces results and is discarded. The high-level API guarantees that *each run
  gets a fresh engine with clean state — no reset needed*.
- **No database, no Redis, no message broker is required for a backtest.** Nautilus *can* use a
  Redis-backed cache and an externally-published message bus, but those are opt-in and aimed at
  *live, multi-node* trading. For an ephemeral backtest, `DatabaseConfig` defaults to `None`.
- **Runs are isolated.** Two backtests share no engine, cache, or bus, so User A's run cannot
  affect User B's.

Put together: one backtest is a perfectly isolated, self-contained unit of work — exactly the
ideal thing to hand to a worker. The only caveat is **determinism across processes**: same data +
same config in the *same* process is reproducible, but *cross-process* reruns are only "expected
to match" and can differ in rare cases. Pin `random_seed` when exact reproduction is required.

---

## 3. The fix: move every `engine.run()` onto a global task queue

To fix the scalability limit, we move each `engine.run()` instance **out of the Flask request
and onto a global task queue, such as [Celery](https://docs.celeryq.dev/).** A pool of
dedicated worker *processes* then consumes that queue and runs the engines in true parallel.

> **What is Celery?** Celery is a distributed task queue for Python. Your application submits
> ("enqueues") a unit of work to a **broker** (commonly Redis or RabbitMQ); a fleet of
> **worker** processes — which can live on many machines — pull tasks off the broker and execute
> them; results and status are written back to a **result backend**. Because each worker is its
> own OS process, each has its own GIL, so CPU-bound tasks run genuinely in parallel.

This maps directly onto the m-cube problem:

| Concept | Maps to in m-cube |
| --- | --- |
| **Task** | One `engine.run()` / one portfolio backtest |
| **Broker** | Redis (or RabbitMQ) holding the queue of pending runs |
| **Worker** | A stateless process that builds a `BacktestEngine`, runs it, writes results |
| **Result backend** | Redis holding job status + a pointer to the results on disk |

The web tier stops doing heavy compute. Instead it **validates the request** (user, allow-lists,
quotas), **enqueues a task**, and immediately returns a `job_id`. Any web replica can later read
that job's status and results back from the broker/result backend. Workers are scaled
**independently** of the web tier — add workers, drain the queue faster.

> Note on terminology: the uploaded design report frames the fix as a generic *"queue + pool of
> stateless worker containers,"* sketched with a thin raw-Redis shim. Celery is a concrete,
> batteries-included implementation of exactly that pattern (retries, acks, autoscaling,
> per-task routing), so the two descriptions are the same architecture at different levels of
> detail.

---

## 4. Target architecture

Three roles, each scaled independently: a stateless **web** tier, a **broker/queue**, and a pool
of stateless **workers**. Shared data lives on mounted volumes.

```
                          ┌──────────────────────────────────┐
   users (browser) ─────► │   reverse proxy / load balancer   │  auth, TLS, X-User-Id
                          └─────────────────┬─────────────────┘
                                            │ HTTP
            ┌───────────────────────────────┼───────────────────────────────┐
            ▼                               ▼                               ▼
     ┌─────────────┐                 ┌─────────────┐                 ┌─────────────┐
     │ web replica │                 │ web replica │      …  N        │ web replica │   gunicorn + Flask
     │ (stateless) │                 │ (stateless) │                 │ (stateless) │   CRUD + ENQUEUE only
     └──────┬──────┘                 └──────┬──────┘                 └──────┬──────┘
            │   enqueue engine.run() task                                   │
            └───────────────────────────────┬───────────────────────────────┘
                                            ▼
                          ┌──────────────────────────────────┐
                          │   Celery broker  (Redis / Rabbit) │   the GLOBAL task queue
                          └─────────────────┬─────────────────┘
                                            │  workers pull tasks
            ┌───────────────────────────────┼───────────────────────────────┐
            ▼                               ▼                               ▼
     ┌──────────────┐                ┌──────────────┐                ┌──────────────┐
     │ Celery worker│                │ Celery worker│      …  M       │ Celery worker│  one OS process each →
     │  engine.run()│                │  engine.run()│                │  engine.run()│  own GIL → true parallelism
     └──────┬───────┘                └──────┬───────┘                └──────┬───────┘
            │  read bars (RO)               │  write results               │
            └───────────────┬───────────────┴───────────────┬───────────────┘
                            ▼                               ▼
                 ┌────────────────────┐        ┌────────────────────────────────┐
                 │   catalog/  (RO)   │        │  portfolios/   reports/  (RWX)  │
                 │ Parquet market data│        │  per-user, namespaced by <uid> │
                 └────────────────────┘        └────────────────────────────────┘
```

**Web service (`N` replicas).** Gunicorn running the existing Flask app. Serves the UI and all
CRUD; for a backtest it validates and enqueues a task, then returns a `job_id`. Stateless, so any
replica serves any request — no session affinity.

**Worker service (`M` replicas).** Same image, different entrypoint: a Celery worker loop that
pulls tasks and **reuses the existing backtest functions unchanged** (e.g.
`run_portfolio_backtest`). Reads bars from the read-only `catalog/`, runs the engine, writes
`reports/<uid>/`. CPU-bound, so size by cores and scale `M` independently of `N`.

**Broker + result backend.** Redis (or RabbitMQ) holds the queue, per-job status, and a pointer
to results — so job state survives a restart and is visible to every replica.

---

## 5. What changes — and what stays exactly the same

The whole point is to keep the **parity-tested execution paths untouched**. The engines are built
the same way and return the same result dictionaries; only the *caller* moves — from "inside the
HTTP request" to "inside a Celery worker."

| Area | Today | After |
| --- | --- | --- |
| HTTP server | Werkzeug dev server, `threaded=True` | Production WSGI server (gunicorn) |
| Backtest execution | `engine.run()` inside the Flask request/thread | `engine.run()` as a Celery task in a worker process |
| Parallelism | Blocked by the GIL (threads serialize) | Real — one process per worker, one GIL each |
| Job state | In-memory dict, lost on restart | Externalized to Redis (survives restart, visible to all replicas) |
| Market data (`catalog/`) | Per-process local cache | Shared read-only volume |
| Per-user data | Local disk | Shared persistent volume, namespaced by `<user_id>` |

The minimal code change is a thin shim: the backtest endpoint enqueues a task instead of running
it inline; a worker consumes the task and calls the **same** function the request calls today.

```python
# WEB (sketch): the endpoint enqueues instead of running inline
task = run_backtest_task.delay(portfolio=cfg, user_id=uid, seed=seed)  # Celery enqueue
return jsonify({"job_id": task.id}), 202

# WORKER (sketch): a Celery task reusing the existing, parity-tested function
@app.task(bind=True)
def run_backtest_task(self, portfolio, user_id, seed=None):
    return run_portfolio_backtest(            # SAME function used today
        portfolio_config=portfolio,
        catalog_path=os.environ["CATALOG_PATH"],
        user_id=user_id,
        # random_seed=seed,                   # pin for cross-process reproducibility
    )
```

---

## 6. Multi-user behavior

"Multiple people run backtests at once" works *because* each run is isolated and the web tier is
stateless:

1. A request lands on **any** web replica (identity comes from the `X-User-Id` header — no session
   affinity needed).
2. Validation (the user's multiplier, allowed-instruments allow-list, portfolio allow-list) runs
   **before** the task is enqueued, so a bad request never reaches a worker.
3. The task is enqueued with its `user_id` in the payload; any worker can pick up any user's task.
4. A worker runs it in full isolation — a fresh engine per run means one user's run cannot affect
   another's.
5. Results land in the user's namespace (`reports/<uid>/`, timestamped), readable by any web
   replica.

**Fairness:** enforce a per-user cap on in-flight tasks at enqueue time (or per-user queues / task
routing in Celery) so one user's parameter sweep cannot starve everyone else.

---

## 7. Risks and caveats

- **Cross-process determinism.** Nautilus guarantees *same-process* reproducibility only;
  cross-process reruns are "expected to match." Pin `random_seed` and reproduce audits in a fresh
  process using the same image when exact equality matters.
- **Spoofable identity.** `X-User-Id` has no authentication on its own. In any deployment beyond a
  trusted LAN, put **real auth at the reverse proxy** and have the proxy set the header — never let
  clients send it directly.
- **Catalog write contention.** CSV ingest is the only writer to the shared `catalog/` and performs
  an unlocked read-modify-write on the venue session-window config. Route ingest through a **single
  serialized worker** (or add a file lock) so concurrent uploads can't clobber each other.
- **Windows → Linux.** The repo is Windows-first; containers/workers are Linux. Verify the Linux
  `pyarrow` / `numpy` wheels match the pinned versions and avoid hard-coded path separators.
- **Per-worker memory.** No fixed figure — profile it, and use the streaming flags
  (`_USE_LAZY_STREAM` / `_USE_UNIFIED_STREAMING`) plus container memory limits for large datasets.

---

## 8. Phased rollout

1. **Phase 0 — Dockerize as-is.** One image, gunicorn instead of the dev server, single container,
   local volumes. Goal: parity with today plus a production server.
2. **Phase 1 — Externalize state & split workers.** Add the broker (Redis); move job state into it;
   introduce the Celery worker and a single serialized ingest worker. The web tier now only enqueues.
3. **Phase 2 — Scale out.** `N` web replicas behind a load balancer; `M` autoscaled workers (scale on
   queue depth or CPU); shared volumes for catalog/portfolios/reports. Add per-user concurrency caps.
4. **Phase 3 (future) — Live trading.** Out of scope here; a stateful service with exchange adapters
   and live reconciliation, scaled differently from the backtest pool.

---

## 9. Sources and further reading

- **m-cube — Containerization & Scalability Report** (uploaded): the full design study this README
  condenses, including the Dockerfile, `docker-compose.yml`, volume strategy, data-flow diagrams,
  and the Kubernetes mapping.
- **NautilusTrader concept docs** (`nautilus_trader==1.224.0`): `Backtesting.txt` (in-process,
  fresh-engine-per-run, streaming API, determinism), `Cache.txt`, `MessageBus.txt`,
  `Live Trading.txt`.
- **"Global Singleton Architecture in NautilusTrader (v1.224.0)"** (internal Notion note): the
  reference for *why* the OS process is the right isolation boundary — i.e., why each portfolio /
  each `engine.run()` gets its own process. *This page is behind a Notion login and could not be
  read automatically; the singleton/per-process reasoning above is inferred from its title and the
  Nautilus docs. If you paste its contents, I can fold its specific claims into §1.1 and §2.*

---

> **Glossary**
>
> - **Backtest** — replaying historical market data through a trading strategy to measure how it
>   would have performed.
> - **`BacktestEngine` / `engine.run()`** — the NautilusTrader object that runs one backtest;
>   `run()` is the CPU-bound loop that does the work.
> - **GIL (Global Interpreter Lock)** — CPython mutex allowing only one thread per process to run
>   bytecode at a time; the reason CPU-bound work must use processes, not threads, to parallelize.
> - **Celery** — a distributed Python task queue: app enqueues tasks to a *broker*; *worker*
>   processes consume and run them.
> - **Broker / worker / result backend** — the queue, the executors, and the status/result store.
> - **`catalog/`** — the shared, read-mostly Parquet store of historical market data.
> - **Embarrassingly parallel** — a workload split into independent pieces that need no
>   coordination, so it scales almost linearly by adding workers.
# Beaver product brief

## Purpose

Beaver is local-first logging infrastructure for understanding what a server
and its applications are doing. It collects metrics, append-only events, and
server logs through language-specific client libraries that send RPC requests
to Beaver. Data is stored locally at first through database adapters and is
explorable through a query engine and local UI.

The aim is to make aggregate behavior, individual events, and human-readable
diagnostic output available in one place and correlated through shared context.

## Core data types

### Metrics

Metrics capture high-throughput measurements associated with a key and an
entity. They are aggregated by the server into configurable fixed-duration
buckets before storage, rather than storing an observation for every request.

Examples of metric instruments:

- flat counter: accumulate (bump by 1) or set a specific number (INT). Useful to represent things point in time (static, enum, # sucsess request, # conc req right now)
  - this can be be exeported to timeseries stats (SUM, COUNT, AVG, RATE)
- quantile stats: stream of values (p99 of x value in the last y time), can sort by P25, P50, P99, etc. Then can also be exported to timeseries similarity. Needs to be backed by more computations/digests, more expensive.

For a 60-second bucket, a `prompt_length` distribution can expose derived
series such as `count`, `sum`, `p50`, and `p99`. The UI may present these as
`prompt_length.p50.60` and `prompt_length.p99.60`; storage should retain the
metric name, statistic, bucket duration, entity, bucket start, and value as
separate fields.

For each time bucket, it should have relevant rolling window step size. For 60 seconds, default should be 1 (this can be changed later.).

Beaver should support selected resolutions such as 5, 10, and 60 seconds.
Finer resolutions cost more storage and compute, so they should be configured
intentionally. Longer-term rollups and retention policies can preserve useful
trends without retaining excessive detail.

### Events (SQL logging)

Events are immutable, append-only structured rows with multiple columns. They
represent things that happened once, such as a request being received. Events
are queryable as tables, samples, and time series, and can optionally belong to
an entity.

### Server logs

Server logs capture conventional diagnostic output: timestamp, level, message,
and structured fields. Local development can also print them to the console.
When available, a log should identify its source file and line number. Server
logs can optionally belong to an entity.

## Entities and tags

An entity is an arbitrary thing being observed, such as a host, process, model,
or service instance. Entities can be hierarchical: a process can belong to a
host, while both may have their own metrics.

Metrics, events, and server logs may be associated with an entity. This enables
an entity view in the UI and lets users drill from a larger entity to its
children.

Metric observations may also include low-cardinality key/value tags, for
example:

```text
log(entity=e1, key=k1, value=42, tags={group: "set_A"})
```

This supports queries for `k1` on `e1` overall, for `e1` filtered to
`group=set_A`, or aggregated across entities in `set_A`. Tags must not contain
unbounded values such as request IDs or user IDs: those create too many metric
series. Such detail belongs in events.

## Product capabilities

- Language client libraries send logging RPCs without exposing storage details.
- High-throughput metric ingestion and server-side bucket aggregation.
- Local database adapters for persisted data.
- A query engine covering metrics, events, logs, entities, and tags.
- A local UI for tables, samples, time series, metric views, and entity views.
- Correlation context, such as request or trace IDs, to connect data types.

## Important constraints

- Logging should not materially slow an application or make it fail when the
  local collector or database is under pressure; clients need buffering and
  backpressure behavior.
- Event schemas and structured log fields need versioning or an evolution
  story.
- The system needs explicit retention, sampling, and cardinality limits.

## Deferred design questions

- Exact entity hierarchy and identity rules.
- Query syntax and transformation/reduction features, including regex-based
  entity selection.
- Which metric resolutions are enabled by default and how long each is kept.
- How to handle delayed observations and whether finalized buckets can change.

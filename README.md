# Auger

A SQL gateway for MongoDB. Speaks the PostgreSQL wire protocol, so psql,
DBeaver, Tableau, Power BI, Metabase, Grafana and every PostgreSQL client
library connect with no driver to install. Query planning and vectorised
execution come from Apache DataFusion and Arrow; as much of each query as can be
expressed faithfully is pushed into MongoDB's aggregation pipeline.

```console
$ psql "postgresql://auger@localhost:5433/shop"
shop=> SELECT status, count(*), round(avg(qty)::numeric, 2) AS avg_qty
shop->   FROM orders GROUP BY status ORDER BY status;
  status   |  n   | avg_qty
-----------+------+---------
 cancelled | 1250 |   51.07
 new       | 1250 |   48.07
 paid      | 1250 |   49.07
 shipped   | 1250 |   50.07
```

## Why not Apache Drill

Drill can already query MongoDB with SQL. Three specific things it gets wrong
are the reason this exists.

### 1. Schema inference that does not fall over

Drill infers a schema per *record batch*, while the query is already running. A
document that introduces a new field or changes a field's type mid-scan aborts
the query with `SchemaChangeException`. In a document database that is not an
edge case — it is Tuesday.

Auger samples up front and freezes the result:

- **Sampling mixes two populations.** A uniform `$sample` is the obvious choice
  and is quietly wrong: in a collection of ten million documents, a field
  present only in the newest thousand has a vanishing chance of being sampled.
  Part of the budget is therefore taken from the `_id` tail. On the test data —
  where `channel` exists in only the last 99 of 5000 documents — this is the
  difference between finding the column and not.
- **Conflicts resolve through a total lattice**, so inference always has an
  answer. `int32` and `int64` widen to `int64`; integers and doubles widen to
  `double`; an `ObjectId` and a string coexist as text. Genuinely incompatible
  types (a field that is a string in some documents and an object in others)
  degrade to canonical extended JSON text. The query keeps running.
- **The result is persisted.** A restart does not silently change a column's
  type underneath a dashboard, and planning does not pay for sampling.

```console
$ auger --describe
schema shop (1 tables)
  orders  (5000 rows)
    _id           Utf8                    bson=objectId
    channel       Utf8                    bson=string      # only in the newest 99 docs
    createdAt     Timestamp(ms, "UTC")    bson=date
    customer      Struct(city, name)      bson=document
    note          Utf8                    bson=json [mixed]  # string in some docs, object in others
    qty           Float64                 bson=double      # int32 in most docs, double in some
    tags          List(Utf8)              bson=array       # array in some docs, bare scalar in others
    total         Decimal128(38, 10)      bson=decimal
```

### 2. Pushdown that is deep *and* correct

Drill's Mongo plugin pushes little more than simple filters and projections, and
evaluates the rest after dragging documents across the network.

Auger translates predicates, projections and limits into the pipeline, and
`EXPLAIN VERBOSE` prints exactly what the server receives — paste it straight
into `mongosh`:

```
MongoExec: shop.orders, partitions=1, pipeline=[
  { "$match": { "$and": [{ "status": { "$eq": "paid" } }, { "orderNo": { "$gt": 4900 } }] } },
  { "$project": { "orderNo": 1, "_id": 0 } }]
```

Depth is worth nothing without correctness, and MongoDB disagrees with SQL in
two places that are easy to get wrong:

**Negation and missing fields.** `{x: {$ne: 5}}` matches documents that have no
`x` at all. SQL evaluates `NULL <> 5` to NULL, which `WHERE` discards. Every
negated predicate is therefore conjoined with `{x: {$ne: null}}`. On the test
data, where `channel` is `'mobile'` in 99 documents and absent in 4901:

| query | naive translation | Auger | correct |
|---|---|---|---|
| `WHERE channel <> 'mobile'` | 4901 | **0** | 0 |
| `WHERE channel NOT IN ('mobile')` | 4901 | **0** | 0 |
| `WHERE NOT (channel = 'mobile')` | 4901 | **0** | 0 |
| `WHERE channel IS NULL` | 4901 | **4901** | 4901 |

**Type-ordered comparison.** Mongo compares across BSON types, so `{x: {$gt: 5}}`
never matches the string `"9"`. Where inference had to coerce a column across
type families, filters on it are reported to the planner as `Inexact` — still
sent to the server so its indexes do the heavy lifting, but re-checked locally.
The planner is only told `Exact` when the `$match` accepts precisely the rows
SQL would; refusing to push down is always safe, and claiming `Exact` wrongly
silently corrupts results.

Literals are built in the representation the *column* uses: comparing an
`ObjectId` column against `'65f...'` emits `ObjectId("65f...")`, not a string,
because the string form matches zero documents without raising an error.

### 3. No JVM, no cluster to operate

One statically-linked binary, one config file. No ZooKeeper, no Drillbits, no GC
pauses. Execution is Arrow-columnar and vectorised, and results stream — a batch
is encoded and flushed as the cursor produces it, so peak memory is one batch
rather than one result set.

Large collections are read by several cursors at once, split on `_id` ranges cut
at sampled quantiles. `_id` always carries a unique index, so every partition
gets an index-driven scan rather than a coordinated collection scan.

## Running it

The host needs no toolchain; everything builds and runs in Docker.

```console
$ docker compose up -d              # mongo + a rust dev container
$ ./x.ps1 test                      # unit tests
$ ./x.ps1 run -- --mongo-uri mongodb://mongo:27017 --listen 0.0.0.0:5433
```

Then connect from anywhere:

```console
$ psql "postgresql://auger@localhost:5433/shop"
```

Useful flags:

| flag | meaning |
|---|---|
| `--describe` | print the discovered catalog and inferred columns, then exit |
| `--mongo-uri` | connection string (also `AUGER_MONGO_URI`) |
| `--listen` | wire-protocol bind address (also `AUGER_LISTEN`) |
| `--sample-size` | documents sampled per collection during inference |
| `--catalog-cache` | file in which inferred schemas persist across restarts |

`--describe` is worth running first: it separates "cannot reach Mongo" and "the
collection is invisible" from "the SQL is wrong", which all look the same from a
client.

See `auger.example.toml` for the full configuration surface.

For a real deployment there are two routes, both against an external MongoDB or
Atlas and both binding the port where it cannot be reached by accident:

- **Docker** — `DEPLOY.ubuntu.md` with `docker-compose.prod.yml`, plus
  `docker-compose.superset.yml` to attach it to an existing Superset network.
- **systemd** — `DEPLOY.systemd.md` with `deploy/install.sh`. One binary in
  `/usr/local/bin`, one unit, no container. Worth preferring when the host has
  no Docker already, or when clients live on other machines and a Docker
  network buys nothing.

Read the security note in whichever you pick before binding the port anywhere
but loopback: there is no TLS, and `auth = "scram"` currently runs the same MD5
handler as `auth = "md5"`.

## Layout

```
src/
  catalog/
    infer.rs      type lattice + sampling -> Arrow schema        (12 tests)
    store.rs      persistent schema cache                        (3 tests)
    provider.rs   Mongo databases as SQL schemas
  mongo/
    pushdown.rs   SQL predicate -> $match, with exactness         (14 tests)
    pipeline.rs   plan -> aggregation pipeline, _id partitioning  (8 tests)
    convert.rs    BSON -> Arrow, schema-driven                    (12 tests)
    provider.rs   TableProvider: filter/projection/limit pushdown
    exec.rs       ExecutionPlan: cursor -> streamed Arrow batches
    client.rs     connection, sampling, statistics                (3 tests)
  server/
    mod.rs        pgwire handlers, streaming responses            (5 tests)
    encode.rs     Arrow -> PostgreSQL types and wire rows         (7 tests)
    compat.rs     SET/BEGIN handling, pg_catalog views            (6 tests)
```

70 unit tests, plus an end-to-end check against a live MongoDB via a real
`psql` client.

## Current limits

Stated plainly, because a gateway that overstates what it supports is worse than
one that refuses clearly.

- **Read-only.** `INSERT`/`UPDATE`/`DELETE` are refused with a message rather
  than failing later with a parse error.
- **Bound parameters are refused.** Extended-protocol portals carrying
  parameters return `0A000` instead of executing with placeholders intact.
  Clients that always use prepared statements are affected.
- **`ORDER BY` and aggregates are not yet pushed down.** They run correctly in
  DataFusion, but reach the scan as operators above it, so pushing them needs a
  physical optimizer rule. The plan and pipeline plumbing for `$sort`, `$skip`
  and `$group` is already in place and unit-tested.
- **`pg_catalog` is partial.** `pg_namespace`, `pg_class`, `pg_attribute`,
  `pg_type`, `pg_index` and `pg_roles` exist as views over the live
  `information_schema`. Clients that reach for more of the catalog may need
  additional relations.
- **Decimal predicates are not pushed.** `bson` 3 exposes no constructor for
  `Decimal128` from a numeric value, so the comparison cannot be expressed
  faithfully; DataFusion evaluates it after the scan.
- **`SET` is accepted and ignored.** Refusing it makes psql and every JDBC
  driver fail before the first query; honouring it is not yet implemented.

## Roadmap

1. `ORDER BY` / `LIMIT` pushdown via a physical optimizer rule, so `$sort` +
   `$limit` collapses into a server-side bounded top-k.
2. `GROUP BY` pushdown to `$group`, the single largest remaining win for
   dashboard workloads.
3. Bound parameters in the extended query protocol.
4. Index-aware costing — `stats.indexed_paths` is already collected but unused.
5. Join pushdown to `$lookup` when both sides live in the same database.

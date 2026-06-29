# RDF store backends

Flyover can run against three interchangeable RDF stores. The application
behaves identically regardless of the chosen backend, because every backend is
published as the container **`rdf-store`** on port **7200** and speaks the same
RDF4J/GraphDB-compatible REST API used by the triplifier.

| Backend  | Reasoning | Compose profile | Notes                                            |
|----------|-----------|-----------------|--------------------------------------------------|
| GraphDB  | native    | `graphdb`       | Performs OWL reasoning itself.                   |
| RDF4J    | none      | `rdf4j`         | Requires materialized inferences (see below).    |
| QLever   | none      | `qlever`        | Fast; reached through a small adapter (see below).|

## Switching backend

The active backend is selected through the `COMPOSE_PROFILES` variable in the
top-level [`.env`](../.env) file. Pick **one** preset, then recreate the stack:

```bash
docker compose down
docker compose up -d --build
```

Each preset also sets:

- `FLYOVER_RDF_STORE_URL` — the base URL the triplifier uses to reach the store.
  This is now the **same** bare URL (`http://rdf-store:7200`) for every backend:
  GraphDB and the QLever adapter expose the REST API at the server root, and the
  Flyover RDF4J image rewrites root-level `/repositories/*` requests to its
  internal `/rdf4j-server` context (see `rdf4j/rewrite.config`). The triplifier
  therefore needs no store-specific suffix.
- `FLYOVER_MATERIALIZE_INFERENCES` — `true` for stores without a reasoner
  (RDF4J, QLever), `false` for GraphDB.

You can also select a profile ad-hoc without editing `.env`:

```bash
COMPOSE_PROFILES=qlever docker compose up -d --build
# or
docker compose --profile qlever up -d --build
```

## Web interface

Regardless of the backend, the store's browser UI is reachable at
[http://localhost:7200](http://localhost:7200):

- **GraphDB** serves its Workbench at the root automatically.
- **RDF4J** normally serves its Workbench under `/rdf4j-workbench`. The Flyover
  RDF4J image replaces Tomcat's default ROOT app with a redirect, so opening the
  bare `http://localhost:7200` lands directly on the Workbench.
- **QLever** is fronted by the adapter; the root path returns a simple health
  message while the triplifier talks to the REST endpoints under it.

## How QLever is integrated

QLever only exposes a single plain SPARQL HTTP endpoint, so it cannot directly
serve the RDF4J/GraphDB REST API (`/repositories/<repo>`,
`/repositories/<repo>/statements`, the Graph Store Protocol, etc.) that the
triplifier expects. The `qlever` profile therefore starts two containers:

- **`qlever-engine`** — the QLever server. On first start it builds an index
  from a minimal seed (`engine/seed.nt`); the real data is then loaded at
  runtime through SPARQL UPDATE. It serves on internal port `7019` and is
  started with an access token so it accepts updates.
- **`rdf-store`** — a thin Flask adapter (`adapter/adapter.py`) that implements
  the RDF4J REST endpoints and translates them onto QLever:
  - SPARQL query   → forwarded as a QLever `query`
  - SPARQL update  → forwarded as a QLever `update` (with the access token)
  - Graph Store Protocol upload/download → translated to
    `INSERT DATA { GRAPH <g> { … } }` / `CONSTRUCT … GRAPH <g>` / `DROP GRAPH <g>`
  - `/protocol` and `/size` probes used during start-up

This keeps the triplifier completely unaware of which store is running.

> **Note on persistence:** QLever applies runtime SPARQL UPDATEs as in-memory
> delta triples. Depending on your QLever version these may not be persisted
> into the on-disk index across `qlever-engine` restarts. The index directory
> is bind-mounted at `rdf-store/qlever/data` so the base index survives
> restarts; verify update persistence against your QLever version before
> relying on it in production.

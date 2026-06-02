# Architecture

This doc maps the running system — what processes exist, how a browser request flows through them, and where data is stored. It also explains two concepts that are load-bearing but easy to get wrong if you don't know them: the GraphDB `userRepo`, and the JSON-LD mapping that ties the workflow steps together.

## System at a glance

Flyover runs as **two containers plus a browser**, defined in [`docker-compose.yml`](../docker-compose.yml):

```mermaid
flowchart LR
    subgraph host["Developer / user machine"]
        browser["Browser<br/>http://localhost:5000/"]
    end
    subgraph net["flyover_network (Docker bridge)"]
        flyover["flyover<br/>(Flask + Gunicorn)<br/>:5000<br/>image: flyover"]
        rdfstore["rdf-store<br/>(GraphDB 10.8.5)<br/>:7200<br/>image: flyover-graphdb"]
    end
    bind[("./graphdb/data<br/>(host bind mount)")]
    browser -- "/ (SPA),<br/>/api/*, /upload, etc." --> flyover
    flyover -- "SPARQL over HTTP<br/>http://rdf-store:7200/repositories/userRepo" --> rdfstore
    rdfstore <--> bind
```

- The browser only ever talks to **`flyover`** on port 5000. Flask serves the Vue SPA bundle (from `/app/flyover/spa/` inside the container) and exposes the JSON API. The SPA's `vue-router` lives at the base path `/`.
- **`flyover`** talks to **`rdf-store`** over the internal Docker network using its hostname `rdf-store`. There is no `host` networking; the bridge `flyover_network` keeps everything isolated from the rest of the host.
- The GraphDB home directory is persisted on the host at `./graphdb/data` (bind mount). That's where the actual triples live across restarts.

In **test mode**, [`docker-compose.test.yml`](../docker-compose.test.yml) overlays this stack to replace the bind mount with a tmpfs volume, so every test run starts from a clean GraphDB. See [`testing.md`](testing.md) for that story.

## Request lifecycle

A typical "user uploads a CSV" round trip:

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser (SPA)
    participant F as Flask (flyover)
    participant S as IngestService
    participant R as RdfStoreRepository
    participant G as GraphDB (rdf-store)

    B->>F: GET /ingest (static SPA route)
    F-->>B: index.html + JS bundle
    Note over B: vue-router renders IngestView.vue
    B->>F: POST /upload (CSV file)
    F->>S: IngestService.handle_upload(...)
    S->>R: RdfStoreRepository.store(triples)
    R->>G: HTTP POST /repositories/userRepo/statements (SPARQL Update)
    G-->>R: 204
    R-->>S: ok
    S-->>F: result dict
    F-->>B: JSON response
    Note over B: Pinia store + StatusBanner show success
```

Two things to notice:
- **Flask serves both the SPA and the API.** The SPA's HTML / JS / CSS are static files copied into the image at build time ([`Dockerfile`](../Dockerfile) stage 1 → stage 2). There is no separate static-asset server in production.
- **The Flask layer is thin.** Controllers do HTTP parsing and call services; services orchestrate; repositories own the SPARQL queries. This is the three-tier shape that lets the unit tests in [`tests/unit/`](../backend/flyover/tests/unit/) mock the repository without touching real GraphDB.

## Data flow: CSV → RDF → SPARQL → UI

Flyover's UI is four steps (Ingest → Describe → Annotate → Share), but it's easier to understand as four **data transformations** with a single durable artefact — the JSON-LD mapping — that survives between steps:

```mermaid
flowchart LR
    csv[(CSV / DB tables)]
    triples[("RDF triples<br/>in userRepo")]
    mapping["JSON-LD mapping<br/>(@context + schema +<br/>variables + valueMapping)"]
    enriched[("RDF +<br/>ontology terms")]
    out[(Exported<br/>JSON-LD / mock data /<br/>DCAT-AP)]

    csv -- "Ingest:<br/>Triplifier converts<br/>rows to triples" --> triples
    triples -- "Describe:<br/>user names variables,<br/>marks types" --> mapping
    mapping -- "Annotate:<br/>user matches variables<br/>to ontology classes<br/>(NCIT, ROO, SIO)" --> enriched
    enriched -- "Share:<br/>export semantic map,<br/>generate mock data,<br/>publish metadata" --> out
```

The JSON-LD mapping is **the** durable document that the user is editing across screens. Every Vue view either reads from it, writes to it, or both. The Flask backend keeps it in `session_cache` ([`main.py`](../backend/flyover/main.py) line 70 onwards), with the browser holding a copy in IndexedDB so reloads don't lose state.

## Backend layers

The Flask side at [`backend/flyover/`](../backend/flyover/) is organised as four layers:

- **[`controllers/`](../backend/flyover/controllers/)** — One Flask Blueprint per workflow step (`ingest_bp`, `describe_bp`, `annotate_bp`, `share_bp`). Each route does HTTP parsing only — pulls JSON or form data, calls a service, packages the response. Many landing routes are now just `redirect("/...")` because the SPA owns the page rendering.
- **[`services/`](../backend/flyover/services/)** — Business logic. `IngestService` runs the Triplifier and writes to GraphDB; `RdfStoreService` is the higher-level wrapper around the repository; `ShareService` does export and mock-data generation; etc. Services are the right place to add new behaviour — they're the layer the unit tests exercise most heavily.
- **[`repositories/`](../backend/flyover/repositories/)** — `RdfStoreRepository` owns the actual HTTP calls to GraphDB's REST API; `query_builder.py` constructs the SPARQL strings. Anything that touches `requests.post("http://rdf-store:7200/...")` belongs here.
- **`session_cache`** — A module-global instance of the `Cache` class in [`main.py`](../backend/flyover/main.py) (line 70, instantiated at line 140). Holds the current user's in-flight state: the loaded CSV, the JSON-LD mapping under construction, the list of variables, etc. It's process-local, so the app is **not** safe to scale horizontally without changes.

If you're hunting a bug, work from the outside in: controller → service → repository. The controller usually just hands the request body to the service.

## What is `userRepo`?

A GraphDB **instance** is one server. Inside it you can create many **repositories** — independent triple stores, each with its own ruleset (RDFS / OWL / none), storage, and access control. They don't share triples. Flyover uses exactly one repository called `userRepo`, configured by [`graphdb/data/data/repositories/userRepo/config.ttl`](../graphdb/data/data/repositories/userRepo/config.ttl).

Why this matters operationally:

- **In production**, `./graphdb/data` is bind-mounted into the container. GraphDB writes its repositories there. Restarting the container preserves data.
- **In tests** (`docker-compose.test.yml`), the bind mount is replaced with a tmpfs and the container starts blank. To make `userRepo` exist on a fresh boot, the `rdf-store` image bakes in a seed at `/opt/graphdb-seed/` (Dockerfile copies [`graphdb/seed/users.js`](../graphdb/seed/users.js), [`graphdb/seed/settings.js`](../graphdb/seed/settings.js), and the `userRepo/config.ttl`). The image's entrypoint, [`graphdb/seed-entrypoint.sh`](../graphdb/seed-entrypoint.sh), does a non-clobbering `cp -rn /opt/graphdb-seed/. /opt/graphdb/home/` before launching GraphDB. On a bind-mounted prod start, that copy skips everything (files already exist); on a fresh tmpfs, it populates the empty store with the seed.

The seed contains stock GraphDB defaults only — single admin user with the documented default password. **Never commit live user state into `graphdb/seed/`.**

The Flask backend learns which repository to use from the env var `FLYOVER_REPOSITORY_NAME=userRepo` (set in [`docker-compose.yml`](../docker-compose.yml)). Changing that without also seeding a config.ttl for the new repository will break ingest.

## What is JSON-LD?

JSON-LD is JSON with one extra key: `@context`. The context maps every other key in the document to a URI, so the same document is both **valid JSON** (parseable by anything) and **an RDF graph** (every key:value becomes a triple). It's how Flyover stores semantic mappings without forcing users to think in triples.

Here's a stripped-down example pulled from [`tests/conftest.py::sample_jsonld_mapping`](../backend/flyover/tests/conftest.py):

```jsonc
{
  "@context": {                                  // tells RDF tools what each key MEANS
    "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
    "schema": "schema/",
    "mapping": "mapping/"
  },
  "@id": "https://flyover.example.org/mapping/test",  // global identifier for this document
  "@type": "mapping:DataMapping",                // RDF class — "this is a DataMapping"
  "name": "Test Mapping",
  "schema": {
    "@type": "schema:SemanticSchema",
    "prefixes": {                                 // ontology prefixes used downstream
      "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
      "roo":  "http://www.cancerdata.org/roo/"
    },
    "variables": {
      "biological_sex": {
        "@type": "schema:CategoricalVariable",
        "predicate": "roo:P100018",               // RDF predicate to use
        "class":     "ncit:C28421",               // ontology class for the variable
        "valueMapping": {
          "terms": {
            "male":   { "targetClass": "ncit:C20197" },  // value → ontology term
            "female": { "targetClass": "ncit:C16576" }
          }
        }
      }
    }
  }
}
```

If you've worked with plain JSON, the only new ideas are: `@id` (global identifier), `@type` (RDF class), `@context` (key → URI map). Everything else is just nested objects. The [json-ld.org primer](https://www.w3.org/TR/json-ld11/) is the canonical 30-minute read.

## Learn more

- [W3C RDF 1.1 Primer](https://www.w3.org/TR/rdf11-primer/) — what triples and SPARQL even are
- [JSON-LD 1.1 Primer](https://www.w3.org/TR/json-ld11/) — the only document you need on JSON-LD itself
- [GraphDB Documentation](https://graphdb.ontotext.com/documentation/10.8/) — especially the "Repositories" and "REST API" sections
- [Flask Blueprints](https://flask.palletsprojects.com/en/stable/blueprints/) — Flyover's controllers are blueprints
- [GO-FAIR — FAIR Principles](https://www.go-fair.org/fair-principles/) — what Flyover is helping users achieve

If something here is wrong or unclear, fix it — that's the easiest contribution you'll ever make.

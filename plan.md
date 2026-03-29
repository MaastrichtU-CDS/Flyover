# MVC Migration Plan — `data_descriptor`

## Problem Statement

The `data_descriptor_main.py` monolith (84 KB, 19 route handlers + helper functions) coexists with a partially-built MVC layer (`controllers/`, `services/`, `repositories/`). Many functions live in the wrong layer or are unused. This plan catalogues everything that should move, and what can be deleted.

---

## Phase 1 — Move Route Handlers from Main Script → Controllers

The main script has 19 `@app.route` decorated functions. Each should be migrated to the appropriate controller blueprint. The main script should become a thin app factory that registers blueprints, initialises the `Cache`, and calls `setup_logging()`.

| Function in `data_descriptor_main.py` | Target Controller | Notes |
|---|---|---|
| `upload_semantic_map()` | `ingest_controller.py` | Handles JSON-LD map uploads |
| `submit_indexeddb_semantic_map()` | `ingest_controller.py` | Frontend IndexedDB map submission |
| `upload_file()` | `ingest_controller.py` | CSV/PostgreSQL uploads |
| `data_submission()` | `ingest_controller.py` | Render submission page |
| `get_graphdb_databases()` | `describe_controller.py` | SPARQL database listing (API) |
| `describe_landing()` | `describe_controller.py` | Landing page |
| `describe_variables()` | `describe_controller.py` | Variable listing page |
| `describe_variable_details()` | `describe_controller.py` | Variable detail page |
| `retrieve_descriptive_info()` | `describe_controller.py` | Fetch metadata from GraphDB |
| `retrieve_detailed_descriptive_info()` | `describe_controller.py` | Fetch detailed metadata |
| `annotation_landing()` | `annotate_controller.py` | Landing page |
| `upload_annotation_json()` | `annotate_controller.py` | Annotation JSON upload |
| `annotation_review()` | `annotate_controller.py` | Review before execution |
| `start_annotation()` | `annotate_controller.py` | Execute annotations |
| `annotation_verify()` | `annotate_controller.py` | Verify results |
| `verify_annotation_ask()` | `annotate_controller.py` | SPARQL ASK verification |
| `favicon()` | `share_controller.py` | Static file serving |
| `custom_static()` | `share_controller.py` | Static file serving |

**Keep in main script:** `setup_logging()`, `Cache` class, Flask app initialisation, blueprint registration.

---

## Phase 2 — Move Business Logic from Main Script → Services

These functions in `data_descriptor_main.py` contain business logic or data-access code that belongs in the service/repository layer:

| Function | Target | Rationale |
|---|---|---|
| `execute_query()` | `repositories/graphdb_repository.py` | Direct SPARQL query execution — repository concern |
| `retrieve_categories()` | `services/describe_service.py` | Fetches value categories — service-level logic |
| `retrieve_global_names()` | `services/describe_service.py` | Gets global variable names from semantic map |
| `formulate_local_semantic_map()` | `services/describe_service.py` | Builds local map from global — pure business logic |
| `handle_postgres_data()` | `services/ingest_service.py` | PostgreSQL connection + ingestion — service logic |
| `insert_equivalencies()` | `services/describe_service.py` | Inserts variable equivalencies — service/repo |
| `allowed_file()` | `services/ingest_service.py` or `utils/file_helpers.py` | File extension validation — utility/service |

---

## Phase 3 — Consolidate `utils/session_helpers.py` into Services

`session_helpers.py` (450 lines, 13 functions) performs service-like work (SPARQL queries, data processing, business decisions). Many of these overlap with existing service classes.

| Function | Target | Notes |
|---|---|---|
| `check_any_data_graph_exists()` | `services/graphdb_service.py` | SPARQL ASK query — service concern |
| `graph_database_ensure_backend_initlisation()` | `services/graphdb_service.py` | Populates session cache from RDF — service |
| `graph_database_fetch_from_rdf()` | `repositories/graphdb_repository.py` | Raw SPARQL query — repository concern |
| `graph_database_find_name_match()` | `services/graphdb_service.py` | Database matching — service logic |
| `graph_database_find_matching()` | `services/graphdb_service.py` | Database matching — service logic |
| `get_table_names_from_mapping()` | `services/ingest_service.py` | Extracts table names from map — service |
| `get_table_names_from_jsonld()` | `services/ingest_service.py` | Helper for above |
| `is_jsonld_semantic_map()` | `loaders/jsonld_loader.py` | Format detection — loader concern |
| `process_variable_for_annotation()` | `services/annotate_service.py` | Annotation enrichment — **may already be duplicated** |
| `get_semantic_map_for_annotation()` | `services/annotate_service.py` | Semantic map selection |
| `has_semantic_map()` | `services/ingest_service.py` or `loaders/` | Boolean check |
| `get_database_name_from_mapping()` | `services/ingest_service.py` | Database name extraction |

**After migration:** `session_helpers.py` can be deleted or reduced to a thin re-export wrapper for backward compatibility during transition.

---

## Phase 4 — Remove Unused Code

These functions are defined but never imported or called anywhere in the codebase:

| Function | File | Action |
|---|---|---|
| `clear_column_mapping_registry()` | `utils/data_preprocessing.py` | **Remove** — never called; was meant for session cleanup but no caller exists |
| `dataframe_to_template_data()` | `utils/data_preprocessing.py` | **Remove** — dead code, not imported anywhere |
| `get_column_mapping()` | `utils/data_preprocessing.py` | **Remove** — exported in `__init__.py` but never used by any consumer |

---

## Phase 5 — Modules That Stay Where They Are

These are already in appropriate locations and need no changes:

| Module | Location | Why it stays |
|---|---|---|
| `loaders/jsonld_loader.py` | `loaders/` | Proper data-loading concern; contains dataclasses and parsing logic |
| `validation/mapping_validator.py` | `validation/` | Proper validation concern; uses jsonschema |
| `utils/data_preprocessing.py` | `utils/` | Cross-cutting utility (encoding detection, column sanitisation) — after dead code removal |
| `annotation_helper/` | `annotation_helper/` | Standalone annotation utility with its own `main.py`; used as a library from `data_descriptor_main.py` |

---

## Execution Order & Dependencies

```
Phase 4 (Remove unused code)          — no dependencies, safe first step
    ↓
Phase 2 (Extract helpers → services)  — moves helper functions to service/repo layer
    ↓
Phase 3 (Consolidate session_helpers) — moves remaining utility functions to services
    ↓
Phase 1 (Move routes → controllers)   — last because routes depend on services being in place
```

**Recommended actual order:** 4 → 2 → 3 → 1 (bottom-up, safest)

---

## Risks & Considerations

- **`Cache` class coupling**: Many route handlers read/write `session_cache` directly. When moving routes to controllers, the `Cache` instance must be injected or made globally accessible via Flask's `g` or app context.
- **Global state in `data_preprocessing.py`**: `_column_mapping_registry` is module-level mutable state. Consider converting to a class or passing via dependency injection.
- **`annotation_helper` standalone usage**: `annotation_helper/main.py` can run as `__main__`. Ensure imports still work if `session_helpers` functions it depends on move.
- **Template references**: HTML templates may use `url_for()` with endpoint names. Changing route registration (main → blueprint) changes endpoint names (e.g., `upload_file` → `ingest.upload_file`). All `url_for()` calls in templates must be updated.
- **Test coverage**: Run `tests/` after each phase to catch regressions. Current test files cover preprocessing, loaders, query builder, services, session helpers, and validation.

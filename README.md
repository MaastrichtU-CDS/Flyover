# Flyover

<p align="center">
<a href="https://doi.org/10.5281/zenodo.17419799"><img alt="DOI: 10.5281/zenodo.17419799" src="https://zenodo.org/badge/DOI/10.5281/zenodo.17419799.svg"></a>
<a href="https://opensource.org/licenses/Apache-2.0"><img alt="Licence: Apache 2.0" src="https://img.shields.io/badge/Licence-Apache%202.0-blue.svg"></a>
<br>
<a href="https://www.ontotext.com/products/graphdb/"><img alt="Ontotext GraphDB 10.8.5" src="https://img.shields.io/badge/Ontotext%20GraphDB-v10.8.5-002b4f.svg"></a>
<a href="https://github.com/MaastrichtU-CDS/Triplifier"><img alt="Triplifier version 2.0.0" src="https://img.shields.io/badge/Triplifier%20Version-v2.0.0-purple"></a>
<br>
<a href="https://www.python.org/downloads/"><img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13+-blue.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://flake8.pycqa.org/"><img alt="Linting: flake8" src="https://img.shields.io/badge/linting-flake8-informational"></a>
<a href="http://mypy-lang.org/"><img alt="Type checking: mypy" src="https://img.shields.io/badge/type%20checking-mypy-informational"></a>
<a href="https://github.com/PyCQA/bandit"><img alt="Security: bandit" src="https://img.shields.io/badge/security-bandit-informational"></a>
<a href="https://github.com/pyupio/safety"><img alt="Security: safety" src="https://img.shields.io/badge/security-safety-informational"></a>
</p>

## Introduction

Flyover is a dockerised Data FAIR-ification tool that transforms structured datasets into semantically enriched,
interoperable formats. It provides a web interface that guides you through the full process — from data ingestion to
semantic annotation and sharing — making your data Findable, Accessible, Interoperable, and Reusable.

Flyover works with CSV files and relational databases, and uses linked data standards such as RDF and JSON-LD for its
semantic representations.

> 📖 **For detailed documentation, see the [Wiki](https://github.com/MaastrichtU-CDS/Flyover/wiki).**
>
> 🛠 **For maintainers and contributors**, the [`docs/`](docs/README.md) folder explains the architecture, the Vue frontend, and the test stack — start with [`docs/architecture.md`](docs/architecture.md).

https://github.com/user-attachments/assets/c6678684-4721-4963-83d1-a91582ce2fe1

## Quick Start

Clone the repository and start Flyover:

```bash
./flyover.sh                    # or: ./flyover.sh start --pull always, for the newest images
```

The script works on Linux and macOS, and on Windows in WSL2 or Git Bash (`sh flyover.sh`). It creates every
container, starts Flyover, and says where to find it. `./flyover.sh stop` stops Flyover and leaves the RDF store
running for anything else that uses it (`--all` stops the store too, which then stays off until you click **Start** in
Flyover), and `./flyover.sh status` shows which containers run and where the store stands. By hand, the same start is `docker compose create` followed by
`docker compose up -d flyover`.

Flyover opens on its home page, which offers its workflows: RDF, and OMOP and CSV, which are still under development.
Entering the RDF workflow asks for the project's JSON-LD map, or to continue without one (see [Projects](#projects)),
and Flyover then starts the RDF store that choice needs: the one a project's map names, or the configured one for the
standard RDF flow. Until then the store's container exists but does not run, so it takes no memory.
Once started, the store comes back after a restart of the machine, until you stop it yourself; Flyover leaves a store
someone stopped alone until you click **Start** in the strip at the top. You can also start it without Flyover, with
`docker compose start rdf-store-graphdb` (or `docker start rdf-store`), or start everything at once with
`docker compose up -d`, as before.

To stop Flyover, use `./flyover.sh stop` (or `docker compose stop`). `docker compose down` removes the containers, the
store's too, and Flyover can start a store but never create one: after a `down`, start Flyover with `./flyover.sh`,
which creates them again.

| Service        | URL                                            | Description                                     |
|----------------|------------------------------------------------|-------------------------------------------------|
| Web interface  | [http://localhost:5000](http://localhost:5000) | Upload and describe your data                   |
| RDF repository | [http://localhost:7200](http://localhost:7200) | Browse the semantic store, once it has started  |

> **Note:** On Windows, please use WSL2 with Docker. On macOS/Linux, Docker can be used directly.

### Choosing the RDF store

Flyover can run on **GraphDB**, **RDF4J**, or **QLever** — the application
behaves identically with any of them. The backend is selected with a single
`COMPOSE_PROFILES` setting in the [`.env`](.env) file (default: `graphdb`):

```bash
# .env
COMPOSE_PROFILES=qlever   # or: graphdb | rdf4j
```

After changing the preset, recreate the stack: `docker compose down`, then `docker compose create --build` and
`docker compose up -d flyover`.
See [`stores/rdf/README.md`](stores/rdf/README.md) for the differences between the
backends and details of the QLever integration.

Flyover reaches the store inside Docker as `http://rdf-store:7200`; its users reach
it where the stack publishes it, `http://localhost:7200`, which is the address the
status bar shows. If you publish the store elsewhere, set `FLYOVER_RDF_STORE_PUBLIC_URL`
to the address your users know it by.

See the wiki's [Getting Started](https://github.com/MaastrichtU-CDS/Flyover/wiki/Getting-Started) page for more details
on configuration and environment variables.

### Repository layout

Top-level directories are named after the *role* a container plays in the stack, not after the technology behind it:

```
flyover.sh             # starts, stops and checks Flyover (Linux, macOS, Windows in WSL2 or Git Bash)
docker-compose.yml     # orchestration: the flyover app + `include:` of every module
app/                   # the Flyover application (Flask backend + Vue frontend, one image)
stores/rdf/            # RDF store backends (graphdb/, rdf4j/, qlever/) + their compose.yml
stores/launcher/       # starts a store's containers once a user chooses to work on it
docs/                  # developer documentation
example_data/          # synthetic datasets and JSON-LD mappings
scripts/               # helper scripts (test stack, clearing GraphDB, ...)
```

Every module the app talks to (currently only the RDF store) lives in its own directory with its own `compose.yml`
and is reached through one stable hostname (`rdf-store`) configured by one `FLYOVER_<MODULE>_URL` environment
variable. New kinds of backends (e.g. `stores/omop/`) or side-car services (e.g. `services/llm/`) follow the same
pattern: a directory, a `compose.yml` pulled in via `include:`, and a Compose profile to switch them on.

## Workflow

Flyover's home page offers its workflows: **RDF**, and **OMOP** and **CSV**, which are under development. The RDF
workflow has its own landing page, **Flyover × RDF** (**Home** in its navigation bar), and guides you through four
distinct steps:

1. **Ingest** — Upload your data (CSV files or connect to a relational database). Flyover converts your data into a
   structured, semantic representation and stores it in the graph database.
2. **Describe** — Provide metadata for your variables: specify data types and add semantic context to each variable in
   your dataset. Optionally supply a
   pre-filled [JSON-LD semantic map](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format) to
   pre-populate descriptions.
3. **Annotate** — Use the information you supplied in the **Describe** step to link your variables to standardised
   ontologies and terminologies using the built-in annotation interface and your project-specific JSON-LD semantic map.
   Alternatively you can use a
   filled-in [JSON-LD semantic map](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format) for your
   data model is needed to drive the annotation process. Review and verify annotations to ensure semantic correctness.
4. **Share** — Download filled-in semantic maps, generate anonymous mock data that preserves the structure of your
   dataset, and share your project with a wider audience.

### Projects

A project is the JSON-LD semantic map a study shares with its sites. Its `schema` section is the general part every
site of the project uses; its `databases` section is the part each site fills in for its own data. Before the RDF
workflow opens, Flyover asks which project to work on: on the home page, when you choose RDF there, or over the page a
link to the workflow led to, which waits for your answer:

- **Continue with** the project this browser remembers, if there is one: it is offered first, with the describe
  answers you added to it.
- **Open a project**: drop or browse for the project's JSON-LD map, or load it from a web address: the file's own, its
  page on Codeberg, GitHub, GitLab or a similar site, or its Zenodo record or DOI. A Zenodo record that holds several
  maps lists them to choose from. You may start from the project's empty template and upload your own filled-in map
  later, in any step.
- **Continue without a project**: the standard RDF flow, on Flyover's configured RDF store, as Flyover always worked.

Cancelling leaves you on the home page, or takes you back to it.

A strip above the steps shows the RDF store Flyover works on, marked **Connected** (green dot) or **Not connected**
(red dot, with the reason on hover), and the open project, with **Switch project** to open another one or to leave it,
and **Exit RDF**, which goes back to the home page. Leaving the workflow, by **Exit RDF** or the browser's Back button,
asks whether to keep the RDF store running or stop it, when the store is Flyover's own and runs: **Keep it running**,
**Stop the RDF store** (its data stays, but nothing can query it until it starts again), or **Stay**. The project stays remembered, to be offered first when you
enter again. **Home** in the workflow's navigation bar stays inside the workflow, so clicking it
shows a small notice beside **Exit RDF** on how to leave, each time until you click **Got it**. On the home page the
strip names neither store nor project. Until you choose, it says **Waiting for a project choice**. While the store starts it says **Starting** (amber dot),
and the step pages that use the store wait in their place, opening once it answers; a page already open stays as it
is. A store someone stopped reads **Stopped**, with a **Start** button there and on the pages that need it. Flyover checks the connection every minute while its tab is visible (every two seconds while the store starts), on
every step, and when you click the status.
Switching and leaving ask first and offer to download the current map, describe answers included. A choice lasts for
the browser tab while it stays in the workflow, so a reload does not ask again; a new tab, or entering again from the
home page, asks again. An open project stays remembered in the browser until you switch or leave it.

Clicking the store opens a small panel with the datasets in it (one per ingested table), their rows and whether they
have been described and annotated. From there you can delete one dataset, every dataset (only Flyover's own graphs; the
rest of the repository stays), or every annotation with the inferences materialised from them. Each delete asks first
and offers to download what goes as a TriG file, which a store can import again.

While a project is open, every step works on the RDF store its map's top-level `endpoint` names, for example
`http://graphdb.example.org:7200/repositories/aya/statements`; without an endpoint, Flyover uses its configured store.
An endpoint on `localhost` or `127.0.0.1` means the configured store with the repository the map names, because inside
Flyover's container `localhost` is the container itself. Flyover never creates repositories: one the store lacks shows
as unreachable in the strip.

Flyover starts its own store only when you choose to work on it: by opening a project on it, or by continuing without
a project. A store on another server is not Flyover's to start. Flyover stops its store only when you ask it to, as
you leave the workflow, since other tools may use the store too. Flyover has no access to Docker itself: a small
launcher service (`stores/launcher/`) starts and stops only the containers the stack labels as a store. If the store runs elsewhere, set `FLYOVER_STORE_LAUNCHER_URL` empty and Flyover takes the
store as running.

A map uploaded in a step is checked against the open project. The project's own map (the same schema) replaces the
working copy, after asking if you added describe answers since it was loaded; another project's map asks whether to
switch. Without a project, a map that belongs to a project offers to open it as one.

The describe and annotate landings show the map the browser holds, the open project's or one uploaded in a step, and
which of its data sources the RDF store holds, so you can go on with it or upload another. The describe landing says
the data was uploaded only right after an ingest; any other visit names the datasets in the store.

### Example Data

The `example_data/` folder contains synthetic datasets and JSON-LD mapping files for two fictitious centres,
demonstrating all supported features. See the
wiki's [Example Data](https://github.com/MaastrichtU-CDS/Flyover/wiki/Example-Data) page for details.

> **Migrating from the old JSON format?**
> See [Migrating from Legacy Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/Migrating-from-Legacy-Format).

## Wiki

The [Wiki](https://github.com/MaastrichtU-CDS/Flyover/wiki) contains detailed documentation on:

- [Getting Started](https://github.com/MaastrichtU-CDS/Flyover/wiki/Getting-Started) — Setup, configuration, and
  workflow
- [JSON-LD Mapping Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/JSON-LD-Mapping-Format) — Variable
  definitions, schema reconstruction, and value mapping
- [Example Data](https://github.com/MaastrichtU-CDS/Flyover/wiki/Example-Data) — Bundled example datasets
- [Migrating from Legacy Format](https://github.com/MaastrichtU-CDS/Flyover/wiki/Migrating-from-Legacy-Format) —
  Converting old JSON mappings to JSON-LD
- [Flyover architecture](https://github.com/MaastrichtU-CDS/Flyover/wiki/Flyover-architecture/Architecture.md) — Flyover's internal architecture 

## Frontend Architecture: Vue.js Progressive Enhancement

Flyover's frontend uses a **progressive enhancement** strategy that combines Flask/Jinja2
server-rendered HTML with Vue.js 3 for interaction-heavy pages. The application architecture
remains Flask-centric, with Python as the primary language for maintainers.

### Overview

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Server rendering** | Flask + Jinja2 | Page structure, navigation, server-side logic |
| **Reactive UI** | Vue.js 3 (vendored) | Complex form state, pagination, live validation |
| **Utilities** | jQuery, Bootstrap | Legacy interactions, selectpicker widgets |
| **Client storage** | IndexedDB (FlyoverDB) | Semantic map persistence across pages |

### Which Pages Use Vue.js

Vue.js components are used on pages with **complex state management** or **heavy client-side
interactivity**:

| Page | Vue Component | Key Features |
|------|--------------|--------------|
| **Ingest** | `vue/ingest-app.js` | CSV file analysis, PK/FK configuration, data linking |
| **Describe Variables** | `vue/describe-variables-app.js` | Pagination, form state caching, auto-populate datatypes |
| **Describe Variable Details** | `vue/describe-variable-details-app.js` | Category/continuous variable forms, IndexedDB sync |
| **Annotation Landing** | `vue/annotation-landing-app.js` | JSON-LD upload, database matching, multi-option UI |
| **Annotation Review** | `vue/annotation-review-app.js` | Semantic map loading, variable cards, pagination |

Pages that remain on the jQuery/vanilla JS backbone (simpler interactions):
- Home (`index.js`), Share pages (`share-landing.js`, `share-mock.js`, `share-publish.js`)
- Navigation (`navigation.js`), shared utilities (`shared-checks.js`, `db-utils.js`)

### How to Add a New Vue.js Component

1. **Create the component file** in `static/js/vue/`:

```javascript
// static/js/vue/my-feature-app.js

const MyFeatureApp = Vue.createApp({
    // Use [[ ]] delimiters to avoid conflicts with Jinja2 {{ }}
    delimiters: ['[[', ']]'],

    template: `
    <div>
        <h3>[[ title ]]</h3>
        <ul>
            <li v-for="item in items" :key="item.id">[[ item.name ]]</li>
        </ul>
        <button @click="addItem">Add Item</button>
    </div>
    `,

    data() {
        return {
            title: 'My Feature',
            items: []
        };
    },

    methods: {
        addItem() {
            this.items.push({ id: Date.now(), name: 'New item' });
        }
    },

    mounted() {
        console.log('Vue component mounted');
    }
});

window.MyFeatureApp = MyFeatureApp;
```

2. **Update the HTML template** (Jinja2):

```html
<!-- In <head>, add Vue.js after jQuery/Bootstrap (served locally, no internet needed) -->
<script src="{{ url_for('static', filename='js/vue.global.prod.js') }}"></script>

<!-- Add a mount point in the page body -->
<div id="my-feature-app"></div>

<!-- Load and mount the component (before </body>) -->
<script src="{{ url_for('static', filename='js/vue/my-feature-app.js') }}"></script>
<script>
    MyFeatureApp.mount('#my-feature-app');
</script>
```

3. **Pass Flask data** to the Vue component:

```html
<script>
    // Option A: Set global variables before mounting
    window.serverData = {{ my_data|tojson }};
    MyFeatureApp.mount('#my-feature-app');

    // Option B: Call an init method after mounting
    const app = MyFeatureApp.mount('#my-feature-app');
    app.init({{ my_data|tojson }});
</script>
```

### Key Conventions

- **Delimiters**: Always use `[[ ]]` for Vue expressions to avoid conflicts with Jinja2's `{{ }}`
- **Vue.js**: Vue.js 3.5.13 is vendored as `static/js/vue.global.prod.js` — no runtime internet connection or build tools required
- **Options API**: Use the Options API (`data()`, `methods`, `computed`, `mounted`) for consistency
- **Templates in JS**: Keep Vue templates in the component's `template` string property, not inline in HTML
- **External utilities**: Continue using `FlyoverDB` and `JSONLDMapper` as global utilities from within Vue methods
- **Form submissions**: Standard HTML form POST remains the submission mechanism; Vue manages the reactive state
- **No SPA routing**: Each page is a separate Flask route with its own Vue app instance

## Third-party logos

The home page shows W3C's [Semantic Web logo](http://www.w3.org/2001/sw/) (the cube) for RDF and the logo of
[OHDSI](https://www.ohdsi.org/), which maintains the OMOP Common Data Model, in white and under their owners' terms:
OHDSI's own white version, and the cube turned white, which W3C allows. Neither W3C nor OHDSI endorses Flyover or is
affiliated with it. See
[`app/backend/data_descriptor/static/logos/README.md`](app/backend/data_descriptor/static/logos/README.md) for where
they come from and the terms they are used under.

## References

- Gouthamchand, V., Choudhury, A., ..., and Wee, L. (2024). Making head and neck cancer clinical data
  Findable-Accessible-Interoperable-Reusable to support multi-institutional collaboration and federated learning .
  *BJR|Artificial Intelligence*, 1(1), ubae005. [doi:10.1093/bjrai/ubae005](https://doi.org/10.1093/bjrai/ubae005)
- Hogenboom, J., Gouthamchand, V., ..., and Lobo Gomes, A. (2026). Knowledge Representation of a Multicenter Adolescent
  and Young Adult Cancer Infrastructure: Development of the STRONG AYA Knowledge Graph *JCO Clinical Cancer
  Informatics*. [doi:10.1200/CCI-25-00177](https://doi.org/10.1200/CCI-25-00177)
- Flyover software archive on Zenodo. [doi:10.5281/zenodo.17419799](https://doi.org/10.5281/zenodo.17419799)

## Developers

- Varsha Gouthamchand
- Joshi Hogenboom
- Tim Hendriks
- Hossein Rahmani
- Johan van Soest
- Leonard Wee

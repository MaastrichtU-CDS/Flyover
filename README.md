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

Clone the repository and start the services:

```bash
docker-compose up -d --pull always
```

| Service        | URL                                            | Description                         |
|----------------|------------------------------------------------|-------------------------------------|
| Web interface  | [http://localhost:5000](http://localhost:5000) | Upload and describe your data       |
| RDF repository | [http://localhost:7200](http://localhost:7200) | Browse the semantic store (GraphDB) |

> **Note:** On Windows, please use WSL2 with Docker. On macOS/Linux, Docker can be used directly.

### Optional: AI mapping suggestions

Flyover can prefill the describe forms with AI-suggested mappings (CSV columns → semantic
variables, categorical values → semantic terms) using a local [Ollama](https://ollama.com/)
model — no data leaves your deployment. Enable it by chaining the LLM compose overlay:

```bash
# CPU inference
docker compose -f docker-compose.yml -f docker-compose.llm.yml up -d

# With an NVIDIA GPU (requires the NVIDIA container runtime)
docker compose -f docker-compose.yml -f docker-compose.llm.yml -f docker-compose.llm-gpu.yml up -d
```

The model (default `llama3.2:3b`) is pulled automatically on first boot and cached in a
Docker volume. Suggestions appear progressively in the describe forms and never overwrite
values you have entered yourself. Without the overlay, no AI features are shown. See
[docs/architecture.md](docs/architecture.md#llm-mapping-suggestions) for how it works and
which `FLYOVER_LLM_*` environment variables tune it.

See the wiki's [Getting Started](https://github.com/MaastrichtU-CDS/Flyover/wiki/Getting-Started) page for more details
on configuration and environment variables.

## Workflow

Flyover guides you through four distinct steps:

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

# Maintainer docs

Documentation for people who hack on Flyover — not for end users running it. End-user docs live in the top-level [`README.md`](../README.md) and the project [Wiki](https://github.com/MaastrichtU-CDS/Flyover/wiki).

Read these in order if you're new:

1. **[architecture.md](architecture.md)** — How the three services (`rdf-store`, `flyover`, browser) fit together, what the data flow looks like, and short primers on JSON-LD, RDF, and the GraphDB `userRepo` concept. Start here.
2. **[frontend.md](frontend.md)** — Vue 3 SPA: the code map, what each composable/store/service does, how the dev server talks to Flask, and how to add a new route. Assumes you know JavaScript and HTTP, but not necessarily Vue.
3. **[testing.md](testing.md)** — Three test layers (pytest, Vitest, Playwright), how to run them locally, the Docker test stack, the CI workflow map, and when to write which kind of test.

Each doc ends with a **Learn more** section linking to the canonical guides for the tools used (Vue, Vite, Pinia, Playwright, Vitest, pytest, json-ld.org, etc.). If a concept here is new to you, those are the rabbit holes.

## Conventions in this docs/ folder

- Diagrams are [Mermaid](https://mermaid.js.org/) and render natively in GitHub's Markdown preview.
- File paths are absolute from the repo root.
- Anything operational (deployment quirks at a specific site, environment-specific config) belongs in the Wiki, not here.

If something in these docs is wrong or unclear, fix it — that's the easiest contribution you'll ever make.

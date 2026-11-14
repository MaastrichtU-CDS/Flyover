# Frontend (Vue 3 SPA)

This doc explains the Vue side of Flyover for someone who knows JavaScript and HTTP but hasn't worked with Vue before. By the end you should know what every file under [`app/frontend/src/`](../app/frontend/src/) does, how the SPA talks to Flask, and how to add a new route.

## The stack in one paragraph

A Single Page Application built with **Vue 3** (the component framework), **Vite** (the dev server and bundler), **Vue Router** (URL-to-component mapping with no full page reloads), **Pinia** (reactive cross-component state — Vue's blessed store library), and **Axios** (HTTP client). Every component is a Single File Component (`.vue`) using the Composition API with `<script setup>` syntax — a leaner alternative to the older "Options API" you might see in tutorials from 2020. The TypeScript toolchain (`vue-tsc`) is present but not strict; `.js` and `.vue` files don't currently use types.

## Code map

```
app/frontend/
├── index.html                       # static shell; <div id="app"> and legacy CSS <link>s
├── package.json                     # npm scripts and deps
├── vite.config.js                   # build config + dev-server proxy to Flask
├── vitest.config.js                 # unit-test config (extends vite.config.js)
├── playwright.config.js             # E2E test config — see testing.md
└── src/
    ├── main.js                      # app entry: createApp → use(pinia, router) → mount('#app')
    ├── App.vue                      # shell: ProjectStatusBar, AppNav, <RouterView/> (or StoreWait), AppFooter, StatusBanner, the project pop-ups
    ├── assets/main.css              # the only Vue-owned CSS (layout overrides)
    ├── router/index.js              # the 11 routes (lazy-loaded), base path /app/; meta.needsStore on the pages that use the store
    ├── views/                       # one component per route
    │   ├── HomeView.vue
    │   ├── IngestView.vue
    │   ├── DescribeLandingView.vue
    │   ├── DescribeVariablesView.vue
    │   ├── DescribeVariableDetailsView.vue
    │   ├── AnnotationLandingView.vue
    │   ├── AnnotationReviewView.vue
    │   ├── AnnotationVerifyView.vue
    │   ├── ShareLandingView.vue
    │   ├── ShareMockView.vue
    │   └── SharePublishView.vue
    ├── components/                  # shared bits used across views
    │   ├── ProjectStatusBar.vue     # strip above the steps: the store and where it stands (waiting, starting, connected...), and the project
    │   ├── StoreWait.vue            # what a page that needs the store shows in its place until the store is there (Start for a stopped one)
    │   ├── ProjectPicker.vue        # pop-up: open a project (file or web address) or continue without one
    │   ├── MapUploadDialog.vue      # the question a step's map upload raises against the open project
    │   ├── StoreDatasetsMenu.vue    # the status bar's store: a panel of its datasets, and their deletes
    │   ├── HoverTip.vue             # the app's tooltip (the home tiles' style, .bootstrap-tooltip)
    │   ├── AppNav.vue               # breadcrumb / step navigator
    │   ├── AppFooter.vue
    │   ├── StatusBanner.vue         # renders Pinia 'status' store messages
    │   └── MigrationStub.vue        # legacy placeholder, gone soon
    ├── composables/
    │   ├── useCyclingDots.js        # the one-to-three dots that show something is under way
    │   ├── useModalFocus.js         # keyboard handling the pop-ups share (focus, Tab, Escape)
    │   ├── useStoreGate.js          # whether the page on screen waits for the store, and why
    │   └── useNavigation.js         # workflow-step state (which steps are completed, disabled)
    ├── lib/
    │   ├── project.js               # which project a map belongs to, its name, a map fingerprint
    │   └── browser.js               # reloadPage() and loadPage(), kept apart so tests can stand in for them
    ├── stores/
    │   ├── project.js               # Pinia store: the open project, kept in step with the server
    │   ├── storeDatasets.js         # Pinia store: the datasets panel's list, question and deletes
    │   └── status.js                # Pinia store: toast/banner messages
    └── services/
        └── api.js                   # axios instance, baseURL '/'
```

## Routes, composables, and the store at a glance

```mermaid
flowchart TD
    subgraph router["router/index.js (base: /app/)"]
        r1["/ → HomeView"]
        r2["/ingest → IngestView"]
        r3["/describe[/variables][/variable-details]"]
        r4["/annotate[/review][/verify]"]
        r5["/share[/mock][/publish]"]
    end
    subgraph state["State"]
        nav["useNavigation()<br/>composable<br/>(step completion flags)"]
        store["useStatusStore()<br/>Pinia store<br/>(toast messages)"]
    end
    api["services/api.js<br/>(axios instance)"]
    flask[("Flask backend<br/>localhost:5000")]

    r1 & r2 & r3 & r4 & r5 --> nav
    r1 & r2 & r3 & r4 & r5 --> store
    nav -->|"GET /api/check-graph-exists"| api
    r2 & r3 & r4 & r5 -->|"POST /upload, /upload-semantic-map, ..."| api
    api --> flask
```

## Projects in the browser

The project store ([`src/stores/project.js`](../app/frontend/src/stores/project.js)) keeps the browser and the server on the same project (see [architecture.md](architecture.md#projects-and-the-store-in-use) for the server side):

- IndexedDB `FlyoverDB` → `metadata` holds the working copy (`semantic_map`, the row the describe pages fill in) and, next to it, the record of the open project (`project`: what the server reported, the token, and the fingerprint of the working copy as it was loaded, which tells whether describe answers were added since).
- `sessionStorage` holds `flyover.project.withoutProject` (this tab chose to work without a project) and `flyover.project.notice` (a message to show after the reload that follows a change).
- When the app starts, `init()` reopens the browser's project on a server that lost it (same token), and opens the pop-up when there is no project and the tab has not chosen. A `BroadcastChannel` (`flyover-project`) tells other tabs about a change.
- Whether the store answers (`/api/v1/project/store-status`) is asked when the app starts, after a change of project, every minute while the tab is visible (`STORE_CHECK_INTERVAL_MS`; a hidden tab asks as soon as it is shown again, if a check is due), on every route change, and when the user clicks the status. A re-check keeps the last answer on screen until the new one arrives.
- A view that takes a map upload calls `project.planUpload(map, fileName)` before it uses the map, and follows the answer: `go: false` means the user kept what was open; `opened: true` means the map is now the project's working copy, so a cleaned copy goes through `project.adoptWorkingCopy(...)` instead of a direct IndexedDB write.

## How dev mode works

Two processes, side by side.

```bash
# terminal 1 — Flask backend (Docker is easiest; or run uv directly)
docker compose up -d flyover   # or: cd app/backend/data_descriptor && uv run python data_descriptor_main.py

# terminal 2 — Vite dev server with HMR
cd app/frontend
npm install
npm run dev                       # serves the SPA at http://localhost:5173/app/
```

Open `http://localhost:5173/app/`. The Vite dev server has HMR (hot module replacement): edit a `.vue` file and the browser re-renders without losing component state.

**Why does the SPA talk to Flask in dev mode?** Because the dev server is a different port (`5173`), normally the browser would block API calls to `5000` as cross-origin. [`vite.config.js`](../app/frontend/vite.config.js) avoids that by proxying a curated list of paths through the dev server itself:

```js
server: {
  proxy: {
    '/api':                          'http://localhost:5000',
    '/upload':                       'http://localhost:5000',
    '/upload-semantic-map':          'http://localhost:5000',
    '/upload-annotation-json':       'http://localhost:5000',
    '/start-annotation':             'http://localhost:5000',
    '/static':                       'http://localhost:5000',
    // …and a few more
  },
}
```

So a SPA call to `/api/check-graph-exists` from `:5173` is silently forwarded to Flask on `:5000`. **If you add a new API route in Flask, you'll likely need to add a proxy entry too**, otherwise it works in production but 404s in dev.

In production, both are served from the same Flask process on `:5000` (Vite isn't running), so the proxy table is irrelevant. The multi-stage [`Dockerfile`](../app/Dockerfile) builds the SPA in stage 1 (Node) and copies `dist/` into `/app/data_descriptor/spa` in stage 2 (Python); Flask serves it as static.

## How to add a new route

Concrete example: adding `/describe/units`.

1. **Create the view component** at `src/views/DescribeUnitsView.vue`:

   ```vue
   <script setup>
   import { ref, onMounted } from 'vue'
   import api from '@/services/api'

   const units = ref([])

   onMounted(async () => {
     const { data } = await api.get('/units')
     units.value = data
   })
   </script>

   <template>
     <div>
       <h2>Units</h2>
       <ul><li v-for="u in units" :key="u">{{ u }}</li></ul>
     </div>
   </template>
   ```

2. **Register the route** in [`src/router/index.js`](../app/frontend/src/router/index.js):

   ```js
   { path: '/describe/units', name: 'describe-units',
     component: () => import('@/views/DescribeUnitsView.vue') },
   ```

   Keep the lazy-import pattern — it keeps the initial bundle small. A page that reads or writes the store also gets
   `meta: STORE` (`{ needsStore: true }`): it then waits in its place while nobody has chosen a project, while the
   store starts and until the first answer about it ([`composables/useStoreGate.js`](../app/frontend/src/composables/useStoreGate.js)),
   so its own checks on mount always meet a store that is there.

3. **(Optional) Surface it in the nav** by adding an entry to `STEPS` in [`composables/useNavigation.js`](../app/frontend/src/composables/useNavigation.js) if it's a top-level workflow step, or just link to it from another view with `<RouterLink to="/describe/units">`.

That's it. Vite picks up the new files on save. If `/units` is a new Flask endpoint too, add a proxy entry to `vite.config.js` so dev mode works.

## Composables vs Pinia: when to use which

A common point of confusion. Both let you share logic between components.

- **A composable** (`use*` function) is a plain JS function that returns reactive refs and computed values. Use it for **stateless or per-call logic**: encapsulating a fetch + parse pattern, deriving step-completion flags from URL state, wrapping `fetch` with retries. Multiple components call the composable independently and get their own state.
  - Example: [`useNavigation()`](../app/frontend/src/composables/useNavigation.js) — derives "which workflow step is active / completed" from the current route. Note the one-shot `primed` flag at the top of the module: it fires `/api/check-graph-exists` exactly once across all consumers, then shares the resulting `dataExists` ref module-wide. This is a deliberate "singleton via module scope" pattern.
- **A Pinia store** is global, shared, reactive state with explicit actions. Use it when **multiple components need to mutate or subscribe to the same state**: a notification queue, a logged-in user, the current JSON-LD mapping.
  - Example: [`useStatusStore()`](../app/frontend/src/stores/status.js) — any component can call `useStatusStore().error('upload failed')` and the message instantly appears in the `StatusBanner` rendered by `App.vue`. There's only one banner; only one store.

Rule of thumb: if removing the feature wouldn't lose information across components, it's a composable; if you'd lose a shared list / queue / object, it's a store.

## Why the legacy CSS

The SPA inherits styling from the pre-Vue (jQuery + Flask templates) era of Flyover. [`index.html`](../app/frontend/index.html) `<link>`s to several stylesheets served by Flask under `/static/css/`: Bootstrap, FontAwesome, plus project-specific `flyover-custom.css` and `share.css`. Those files live under [`app/backend/data_descriptor/static/`](../app/backend/data_descriptor/static/), not in the frontend tree.

The Vue side only owns [`src/assets/main.css`](../app/frontend/src/assets/main.css) (layout overrides) and scoped CSS inside each `.vue` file's `<style scoped>` block. Component-local styles use scoped CSS; project-wide visual changes mean editing files under the Flask `static/` tree.

This is awkward but deliberate: replacing Bootstrap and the design tokens is a much bigger project than the SPA migration. Keep new component styles `<style scoped>` and the boundary stays manageable.

## Learn more

- [Vue 3 guide — Introduction](https://vuejs.org/guide/introduction.html) — start here if Vue is new. Read at minimum the **Essentials** chapter (reactivity, components, lifecycle).
- [Vue Composition API & `<script setup>`](https://vuejs.org/api/sfc-script-setup.html) — every `.vue` file in this repo uses this style.
- [Vue Router 4](https://router.vuejs.org/) — focus on the **Essentials**: dynamic routes, navigation, `<RouterView>`, `<RouterLink>`.
- [Pinia](https://pinia.vuejs.org/core-concepts/) — short docs. We use the "setup store" pattern (a function passed to `defineStore`), not the older options-object pattern.
- [Vite guide](https://vitejs.dev/guide/) — read the **Server Options** section to understand the dev-server proxy.
- [Axios](https://axios-http.com/docs/intro) — only the basics; we don't use interceptors yet.
- [Vue Mastery — Vue 3 Tutorial (free)](https://www.vuemastery.com/courses/intro-to-vue-3/intro-to-vue3/) — a video walkthrough if reading docs isn't your thing.

If something here is wrong or unclear, fix it — that's the easiest contribution you'll ever make.

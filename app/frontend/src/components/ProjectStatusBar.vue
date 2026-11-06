<script setup>
import { computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useProjectStore } from '@/stores/project'

// The strip above the step bar: which RDF store Flyover works on (and
// whether it answers), which project is open, and the way to switch.

const projectStore = useProjectStore()
const { store, storeStatus, project, name, version } = storeToRefs(projectStore)

const dotClass = computed(() => `is-${storeStatus.value.state}`)

// Said in words too, so the state never rests on the dot's colour alone.
const connectionLabel = computed(
  () =>
    ({
      reachable: 'Connected',
      unreachable: 'Not connected',
    })[storeStatus.value.state] || 'Checking…'
)

const storeTitle = computed(() => {
  if (!store.value) return 'Flyover has not heard from the server yet.'
  const lines = [`RDF store: ${store.value.url}`]
  lines.push(
    store.value.source === 'project'
      ? "Named by the open project's map."
      : "Flyover's configured store."
  )
  if (store.value.note) lines.push(store.value.note)
  if (storeStatus.value.state === 'unreachable' && storeStatus.value.reason) {
    lines.push(storeStatus.value.reason)
  }
  return lines.join('\n')
})

const projectTitle = computed(() => {
  const open = project.value
  if (!open) return 'Flyover works without a project: the standard RDF flow.'
  const lines = [[name.value, version.value].filter(Boolean).join(' · ')]
  if (open.description) lines.push(open.description)
  if (open.schemaName) lines.push(`Schema: ${open.schemaName}`)
  if (open.sourceUrl) lines.push(`From: ${open.sourceUrl}`)
  else if (open.fileName) lines.push(`File: ${open.fileName}`)
  return lines.join('\n')
})
</script>

<template>
  <div
    id="project-status-bar"
    class="flyover-statusbar"
  >
    <!-- The page content's container, so the strip lines up with it. -->
    <div class="container container-flyover statusbar-inner">
      <span
        id="statusbar-store"
        class="statusbar-item"
        :title="storeTitle"
      >
        <i
          class="fas fa-database"
          aria-hidden="true"
        />
        RDF store<template v-if="store">
          · <strong>{{ store.repository }}</strong>
        </template>
        <span aria-hidden="true">·</span>
        <span
          id="statusbar-connection"
          class="statusbar-connection"
        >
          <span
            class="statusbar-dot"
            :class="dotClass"
            aria-hidden="true"
          />
          <span class="sr-only">Connection to the store:</span>
          {{ connectionLabel }}
        </span>
      </span>
      <span
        id="statusbar-project"
        class="statusbar-item"
        :title="projectTitle"
      >
        <i
          class="fas fa-folder-open"
          aria-hidden="true"
        />
        <template v-if="project">
          Project: <strong>{{ name }}</strong><template v-if="version">
            · {{ version }}
          </template>
        </template>
        <template v-else>
          No project
        </template>
        <span aria-hidden="true">·</span>
        <button
          id="statusbar-switch"
          type="button"
          class="statusbar-link"
          @click="projectStore.openPicker('switch')"
        >
          {{ project ? 'Switch project' : 'Choose a project' }}
        </button>
      </span>
    </div>
  </div>
</template>

<style scoped>
/* A darker shade of the step bar's gradient, so the two read as one header. */
.flyover-statusbar {
  background: linear-gradient(135deg, #4d5fb0 0%, #59387a 100%);
  color: rgba(255, 255, 255, 0.9);
  font-size: 0.8rem;
  line-height: 1.4;
}

.statusbar-inner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.25rem 1.5rem;
  padding-top: 0.3rem;
  padding-bottom: 0.3rem;
}

.statusbar-item {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.35rem;
}

.statusbar-item strong {
  color: #fff;
}

.statusbar-link {
  padding: 0;
  border: 0;
  background: none;
  color: #fff;
  font: inherit;
  text-decoration: underline;
  text-underline-offset: 2px;
  cursor: pointer;
}

.statusbar-link:hover {
  text-decoration-thickness: 2px;
}

.statusbar-link:focus-visible {
  outline: 2px solid rgba(255, 255, 255, 0.85);
  outline-offset: 2px;
  border-radius: 2px;
}

.statusbar-connection {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
}

/* A thin dark edge sets the dot off the purple without the whitish halo a
   light ring gave it. */
.statusbar-dot {
  display: inline-block;
  width: 0.55rem;
  height: 0.55rem;
  border-radius: 50%;
  background: #adb5bd;
  box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.3);
}

.statusbar-dot.is-reachable {
  background: #28a745;
}

.statusbar-dot.is-unreachable {
  background: #dc3545;
}

.statusbar-dot.is-checking {
  animation: statusbar-pulse 1s ease-in-out infinite alternate;
}

@keyframes statusbar-pulse {
  from {
    opacity: 0.4;
  }
  to {
    opacity: 1;
  }
}

@media (prefers-reduced-motion: reduce) {
  .statusbar-dot.is-checking {
    animation: none;
  }
}
</style>

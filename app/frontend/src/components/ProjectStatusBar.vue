<script setup>
import { computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import HoverTip from '@/components/HoverTip.vue'
import StoreDatasetsMenu from '@/components/StoreDatasetsMenu.vue'
import { useCyclingDots } from '@/composables/useCyclingDots'
import { useProjectStore } from '@/stores/project'
import { useStoreDatasetsStore } from '@/stores/storeDatasets'

// The strip above the step bar: which RDF store Flyover works on (and
// where it stands; a click on it opens its datasets), which project is
// open, and the way to switch.
// Where the store stands is asked again on every step, every minute while
// the tab is visible (every two seconds while it starts; see the project
// store), and on a click. Until someone chooses a project, or the standard
// RDF flow, Flyover starts no store and the strip says it is waiting. On the
// home page no workflow is chosen, so the strip names neither store nor
// project; inside a workflow it offers to exit it, back to the home page.

const projectStore = useProjectStore()
const route = useRoute()
const router = useRouter()
const { store, storeStatus, project, name, version, picker } = storeToRefs(projectStore)
// While the datasets panel is open, the store's tooltip stays away from it.
const { open: datasetsOpen } = storeToRefs(useStoreDatasetsStore())

// The page belongs to a workflow (the RDF one), not to the home page.
const inWorkflow = computed(() => Boolean(route.meta?.workflow))

// Nobody has chosen what to work on: the server said so, or this tab's
// pop-up asks as it enters the workflow.
const waiting = computed(
  () =>
    storeStatus.value.state === 'waiting' ||
    (picker.value.open && picker.value.mode === 'enter')
)

// Leave the workflow for the home page; the store keeps running and the
// project stays remembered.
function exitWorkflow() {
  projectStore.exitWorkflow()
  router.push('/')
}
// The work uses no database (none does yet: a later workflow may keep
// everything in memory).
const noDatabase = computed(() => storeStatus.value.state === 'none')

const dotClass = computed(() => [
  `is-${storeStatus.value.state}`,
  { 'is-checking': storeStatus.value.checking },
])

watch(
  () => route.path,
  () => projectStore.refreshStoreStatus()
)

// Said in words too, so the state never rests on the dot's colour alone.
// Starting and connecting get the cycling dots: something is under way.
const connectionLabel = computed(
  () =>
    ({
      reachable: 'Connected',
      unreachable: 'Not connected',
      starting: 'Starting',
      stopped: 'Stopped',
    })[storeStatus.value.state] || 'Connecting'
)
// Someone stopped Flyover's store: it stays so until the user starts it.
const stopped = computed(() => storeStatus.value.state === 'stopped')
const connectionUnderWay = computed(() =>
  ['starting', 'unknown'].includes(storeStatus.value.state)
)
const dots = useCyclingDots(connectionUnderWay)

// The store's hover: where it is, whose map named it, and why it cannot be
// reached when it cannot.
const storeTip = computed(() => {
  if (!store.value) return 'Flyover has not heard from the server yet.'
  const lines = [store.value.url]
  if (store.value.source === 'project') lines.push("Named by the open project's map.")
  if (storeStatus.value.state === 'starting') {
    lines.push('Flyover is starting the store; this can take a minute.')
  }
  if (['unreachable', 'stopped'].includes(storeStatus.value.state) && storeStatus.value.reason) {
    lines.push(storeStatus.value.reason)
  }
  return lines.join('\n')
})

const projectTip = computed(() => {
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
        v-if="!inWorkflow"
        id="statusbar-home"
        class="statusbar-item"
      >
        <i
          class="fas fa-home"
          aria-hidden="true"
        />
        Home
        <span aria-hidden="true">·</span>
        <span class="statusbar-muted">Choose a workflow to begin</span>
      </span>
      <span
        v-else-if="waiting"
        id="statusbar-store"
        class="statusbar-item statusbar-waiting"
      >
        <HoverTip
          text="Flyover starts a store once you choose a project, or continue without one."
          placement="below"
          align="start"
          wrap
        >
          <i
            class="fas fa-hourglass-half"
            aria-hidden="true"
          />
          Waiting for a project choice
        </HoverTip>
      </span>
      <span
        v-else-if="noDatabase"
        id="statusbar-store"
        class="statusbar-item"
      >
        <i
          class="fas fa-database"
          aria-hidden="true"
        />
        No database in use
      </span>
      <span
        v-else
        id="statusbar-store"
        class="statusbar-item"
      >
        <HoverTip
          :text="datasetsOpen ? '' : storeTip"
          placement="below"
          align="start"
          wrap
        >
          <StoreDatasetsMenu>
            <i
              class="fas fa-database"
              aria-hidden="true"
            />
            RDF store<template v-if="store">
              · <strong>{{ store.repository }}</strong>
            </template>
          </StoreDatasetsMenu>
        </HoverTip>
        <span aria-hidden="true">·</span>
        <HoverTip
          v-slot="{ describedby }"
          text="Check the connection again"
          placement="below"
        >
          <button
            id="statusbar-connection"
            type="button"
            class="statusbar-connection"
            :aria-describedby="describedby"
            @click="projectStore.refreshStoreStatus()"
          >
            <span
              class="statusbar-dot"
              :class="dotClass"
              aria-hidden="true"
            />
            <!-- The space sits inside: the template drops those between tags. -->
            <span class="sr-only">{{ 'Connection to the store: ' }}</span>
            <!-- One flex item, so the dots sit right after the word. -->
            <span>{{ connectionLabel }}<span
              v-if="connectionUnderWay"
              class="statusbar-dots"
              aria-hidden="true"
            >{{ '.'.repeat(dots) }}</span></span>
          </button>
        </HoverTip>
        <template v-if="stopped">
          <span aria-hidden="true">·</span>
          <button
            id="statusbar-start"
            type="button"
            class="statusbar-link"
            :disabled="storeStatus.checking"
            @click="projectStore.startStore()"
          >
            Start
          </button>
        </template>
      </span>
      <span
        v-if="inWorkflow"
        id="statusbar-project"
        class="statusbar-item"
      >
        <HoverTip
          :text="projectTip"
          placement="below"
          align="end"
          wrap
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
        </HoverTip>
        <span aria-hidden="true">·</span>
        <button
          id="statusbar-switch"
          type="button"
          class="statusbar-link"
          @click="projectStore.openPicker('switch')"
        >
          {{ project ? 'Switch project' : 'Choose a project' }}
        </button>
        <span aria-hidden="true">·</span>
        <button
          id="statusbar-exit"
          type="button"
          class="statusbar-link"
          @click="exitWorkflow"
        >
          <i
            class="fas fa-right-from-bracket"
            aria-hidden="true"
          /> Exit RDF
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
  padding: 0;
  border: 0;
  background: none;
  color: inherit;
  font: inherit;
  cursor: pointer;
}

.statusbar-connection:focus-visible {
  outline: 2px solid rgba(255, 255, 255, 0.85);
  outline-offset: 2px;
  border-radius: 2px;
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

.statusbar-dot.is-starting {
  background: #ffc107;
}

.statusbar-dot.is-checking,
.statusbar-dot.is-starting,
.statusbar-dot.is-unknown {
  animation: statusbar-pulse 1s ease-in-out infinite alternate;
}

/* Room for three dots, so the label does not shift as they count. */
.statusbar-dots {
  display: inline-block;
  width: 1.1em;
  text-align: left;
}

.statusbar-waiting,
.statusbar-muted {
  font-style: italic;
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
  .statusbar-dot.is-checking,
  .statusbar-dot.is-starting,
  .statusbar-dot.is-unknown {
    animation: none;
  }
}
</style>

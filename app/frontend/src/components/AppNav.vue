<script setup>
import { computed } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useNavigation } from '@/composables/useNavigation'
import { useWorkflowEntry } from '@/composables/useWorkflowEntry'
import { WORKFLOWS } from '@/lib/workflows'

// The bar under the status strip. On the home page it offers the workflows,
// which ask for the project before they open (see useWorkflowEntry), and no
// Home item: every item leads away from the page it would point to. Inside
// the RDF workflow it shows "RDF home" and the workflow's steps, which
// another workflow may not share.

const route = useRoute()
const { stepStates } = useNavigation()
const { enter } = useWorkflowEntry()

const inRdf = computed(() => route.meta?.workflow === 'rdf')
</script>

<template>
  <div class="flyover-navigation">
    <div class="container">
      <div
        v-if="inRdf"
        class="nav-steps"
      >
        <RouterLink
          id="rdf-home-step"
          to="/rdf"
          class="nav-step"
          :class="{ active: route.path === '/rdf' }"
        >
          <span class="step-number">
            <i class="fas fa-project-diagram step-icon-header" />
          </span>
          <span class="step-text">RDF home</span>
        </RouterLink>

        <span class="step-separator">|</span>

        <template
          v-for="(step, idx) in stepStates"
          :key="step.name"
        >
          <RouterLink
            :id="`${step.name}-step`"
            :to="step.to"
            class="nav-step"
            :class="{
              active: step.active,
              completed: step.completed,
              disabled: step.disabled,
            }"
            @click.prevent="step.disabled ? null : null"
          >
            <span class="step-number">
              <i
                class="fas step-icon-header"
                :class="step.completed ? step.completedIcon : step.icon"
              />
            </span>
            <span class="step-text">{{ step.label }}</span>
          </RouterLink>
          <i
            v-if="idx < stepStates.length - 1"
            class="fas fa-chevron-right step-arrow"
          />
        </template>
      </div>

      <div
        v-else
        class="nav-steps"
      >
        <template
          v-for="(workflow, idx) in WORKFLOWS"
          :key="workflow.id"
        >
          <span
            v-if="idx > 0"
            class="step-separator"
          >|</span>
          <a
            :id="`${workflow.id}-workflow-step`"
            href="#"
            role="button"
            class="nav-step"
            @click.prevent="enter(workflow)"
          >
            <!-- The tile's logo in white, as the steps' icons inside a
                 workflow: the name beside it says what it is. -->
            <span class="step-number workflow-disc">
              <img
                v-if="workflow.logo"
                :src="workflow.logo.src"
                :class="{ 'is-whitened': workflow.logo.whiten }"
                alt=""
              >
              <i
                v-else
                class="fas"
                :class="workflow.icon"
                aria-hidden="true"
              />
            </span>
            <span class="step-text">{{ workflow.label }}</span>
          </a>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.workflow-disc {
  color: #fff;
}

/* The old stylesheet gives every icon a right margin, which pushes it off
   the centre of its circle. */
.workflow-disc i {
  margin: 0;
}

.workflow-disc img {
  width: 18px;
  height: 18px;
  object-fit: contain;
}

/* A logo whose owner allows other colours (W3C's cube), turned white. */
.workflow-disc img.is-whitened {
  filter: brightness(0) invert(1);
}
</style>

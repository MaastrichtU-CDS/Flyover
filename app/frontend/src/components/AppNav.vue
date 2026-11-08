<script setup>
import { computed } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useNavigation } from '@/composables/useNavigation'
import { WORKFLOWS } from '@/lib/workflows'
import { useWorkflowsStore } from '@/stores/workflows'

// The bar under the status strip. On the home page it offers the workflows;
// inside the RDF workflow it shows "RDF home" and the workflow's steps,
// which another workflow may not share.

const route = useRoute()
const { stepStates } = useNavigation()
const workflows = useWorkflowsStore()

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
        <RouterLink
          id="home-step"
          to="/"
          class="nav-step"
          :class="{ active: route.path === '/' }"
        >
          <span class="step-number">
            <i class="fas fa-home step-icon-header" />
          </span>
          <span class="step-text">Home</span>
        </RouterLink>

        <template
          v-for="workflow in WORKFLOWS"
          :key="workflow.id"
        >
          <span class="step-separator">|</span>
          <RouterLink
            v-if="workflow.available"
            :id="`${workflow.id}-workflow-step`"
            :to="workflow.to"
            class="nav-step"
          >
            <span class="step-number">
              <i
                class="fas step-icon-header"
                :class="workflow.icon"
              />
            </span>
            <span class="step-text">{{ workflow.label }}</span>
          </RouterLink>
          <a
            v-else
            :id="`${workflow.id}-workflow-step`"
            href="#"
            role="button"
            class="nav-step"
            @click.prevent="workflows.showSoon(workflow.id)"
          >
            <span class="step-number">
              <i
                class="fas step-icon-header"
                :class="workflow.icon"
              />
            </span>
            <span class="step-text">{{ workflow.label }}</span>
          </a>
        </template>
      </div>
    </div>
  </div>
</template>

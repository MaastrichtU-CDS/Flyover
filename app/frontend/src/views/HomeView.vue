<script setup>
// Flyover's home page: the workflows, each a tile. Choosing RDF asks for the
// project's JSON-LD here first, and then goes on to the workflow's own
// landing page (RdfLandingView); see useWorkflowEntry. OMOP and CSV are
// under development, and say so.
import { useWorkflowEntry } from '@/composables/useWorkflowEntry'
import { WORKFLOWS, workflowById } from '@/lib/workflows'

const { enter } = useWorkflowEntry()
const rdf = workflowById('rdf')
</script>

<template>
  <div>
    <div class="hero-section">
      <h1 class="hero-title">
        Welcome to Flyover
      </h1>
      <p class="hero-subtitle">
        A privacy-aware tool to transform your data into F.A.I.R. semantically interoperable
        resources
      </p>
      <p class="mb-0">
        Choose how to work with your data: as RDF triples in an RDF store, in the OMOP common
        data model, or as a mapped CSV file
      </p>
    </div>

    <div class="row justify-content-center">
      <div
        v-for="workflow in WORKFLOWS"
        :key="workflow.id"
        class="col-xl-4 col-lg-4 col-md-6 mb-4"
      >
        <div
          :id="`${workflow.id}-card`"
          class="workflow-card"
          :class="{ 'is-coming': !workflow.available }"
          role="button"
          tabindex="0"
          @click="enter(workflow)"
          @keyup.enter="enter(workflow)"
        >
          <span
            v-if="!workflow.available"
            class="badge workflow-badge"
          >Under development</span>
          <div
            class="step-icon"
            :class="workflow.iconClass"
          >
            <i
              class="fas"
              :class="workflow.icon"
            />
          </div>
          <h3 class="step-title">
            {{ workflow.label }}
          </h3>
          <p class="step-description">
            {{ workflow.description }}
          </p>
          <ul class="step-features">
            <li
              v-for="feature in workflow.features"
              :key="feature"
            >
              <i class="fas fa-check" /> {{ feature }}
            </li>
          </ul>
        </div>
      </div>
    </div>

    <div class="cta-section">
      <h3><i class="fas fa-rocket text-primary" /> Ready to Get Started?</h3>
      <p class="mb-4">
        Bring your project's JSON-LD semantic map, or start without one, and Flyover starts
        the store your work needs.
      </p>
      <button
        id="home-start"
        type="button"
        class="btn btn-primary btn-lg"
        @click="enter(rdf)"
      >
        <i class="fas fa-play" /> Start Data F.A.I.R.-ification Process
      </button>
    </div>
  </div>
</template>

<style scoped>
.hero-section::before {
  background-image: url('/static/image-flyover.svg');
}

.workflow-card {
  position: relative;
  cursor: pointer;
}

/* A workflow under development still opens its note, so it stays a card
   one can click, a little quieter than the one that is built. */
.workflow-card.is-coming .step-icon {
  opacity: 0.75;
}

.workflow-badge {
  position: absolute;
  top: 0.75rem;
  right: 0.75rem;
  background: rgba(118, 75, 162, 0.12);
  color: #764ba2;
  font-weight: 500;
}
</style>

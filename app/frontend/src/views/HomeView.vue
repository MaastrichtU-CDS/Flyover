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
      <p class="hero-subtitle mb-0">
        A privacy-aware tool to transform your data into F.A.I.R. semantically interoperable
        resources
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
          role="button"
          tabindex="0"
          @click="enter(workflow)"
          @keyup.enter="enter(workflow)"
        >
          <span
            v-if="!workflow.available"
            class="badge workflow-badge"
          >Under development</span>
          <!-- The workflow's logo in white, as the steps' icons on the RDF
               landing. -->
          <div
            class="step-icon"
            :class="workflow.iconClass"
          >
            <img
              v-if="workflow.logo"
              class="workflow-logo"
              :class="{ 'is-whitened': workflow.logo.whiten }"
              :src="workflow.logo.src"
              :alt="workflow.logo.alt"
            >
            <i
              v-else
              class="fas"
              :class="workflow.icon"
              aria-hidden="true"
            />
          </div>
          <h3 class="step-title">
            {{ workflow.label }}
          </h3>
          <!-- The text grows, so the lists below line up across the tiles. -->
          <div class="workflow-text">
            <p class="step-description">
              {{ workflow.description }}
            </p>
            <p
              v-if="workflow.note"
              class="workflow-note"
            >
              <!-- Each line on a line of its own. A link opens beside Flyover,
                   and does not open the tile. The parts carry their own
                   spaces (v-text keeps the template's line breaks out of the
                   sentence). -->
              <span
                v-for="(line, l) in workflow.note"
                :key="l"
                class="workflow-note-line"
              >
                <template
                  v-for="(part, i) in line"
                  :key="i"
                >
                  <a
                    v-if="part.href"
                    :href="part.href"
                    target="_blank"
                    rel="noopener noreferrer"
                    @click.stop
                    @keyup.enter.stop
                    v-text="part.text"
                  />
                  <span
                    v-else
                    v-text="part"
                  />
                </template>
              </span>
            </p>
          </div>
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
        Use a shared JSON-LD to align your data with partners, or familiarise yourself with the
        workflow without one.
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

/* The old stylesheet gives every icon a right margin, which pushes it off
   the centre of its circle. */
.step-icon > i {
  margin: 0;
}

/* As large as the steps' icons look in their circles. */
.workflow-logo {
  width: 40px;
  height: 40px;
  object-fit: contain;
}

/* A drawing in black (RDF's knowledge graph), turned white. */
.workflow-logo.is-whitened {
  filter: brightness(0) invert(1);
}

.workflow-text {
  flex-grow: 1;
  width: 100%;
}

/* Small print, close under the description it qualifies. */
.workflow-note-line {
  display: block;
}

.workflow-note {
  margin: -0.75rem 0 1.25rem;
  color: #6c757d;
  font-size: 0.75rem;
  text-align: center;
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

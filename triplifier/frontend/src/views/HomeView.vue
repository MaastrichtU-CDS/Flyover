<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useNavigation } from '@/composables/useNavigation'
import { useStatusStore } from '@/stores/status'

const router = useRouter()
const { dataExists } = useNavigation()
const status = useStatusStore()

const showTooltip = ref(null)

const cards = [
  {
    id: 'ingest',
    to: '/ingest',
    iconClass: 'ingest-icon',
    titleIcon: 'fa-cookie-bite',
    title: '1. Ingest',
    description:
      'Upload and process your datasets to prepare them for semantic annotation and interoperability.',
    features: [
      'CSV and DB support',
      'Automatic conversion',
      'Data grooming',
      'Link multiple graphs',
    ],
  },
  {
    id: 'describe',
    to: '/describe',
    iconClass: 'describe-icon',
    titleIcon: 'fa-edit',
    title: '2. Describe',
    description:
      'Provide metadata and descriptions for your data variables to enhance semantic meaning.',
    features: [
      'Data type specifications',
      'Unit specifications',
      'Local definitions',
      'Variable labelling',
    ],
    requiresData: true,
  },
  {
    id: 'annotate',
    to: '/annotate',
    iconClass: 'annotate-icon',
    titleIcon: 'fa-tags',
    title: '3. Annotate',
    description:
      'Annotate your data using standardised ontologies to make your data semantically interoperable.',
    features: [
      'Ontological annotations',
      'Semantic interoperability',
      'Metadata-driven',
      'Value mapping',
    ],
    requiresData: true,
  },
  {
    id: 'share',
    to: '/share',
    iconClass: 'share-icon',
    titleIcon: 'fa-mail-forward',
    title: '4. Share',
    description:
      'Share (meta-)data of your semantically interoperable data and speed up your collaborations.',
    features: [
      'Archive mapping',
      'Generate mock data',
      'Increase findability',
      'DCAT-AP export',
    ],
  },
]

function isDisabled(card) {
  return !!card.requiresData && !dataExists.value
}

function open(card) {
  if (isDisabled(card)) {
    status.warning(
      'Please complete the Ingest step first by submitting your data.'
    )
    return
  }
  router.push(card.to)
}
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
        Transform your data into F.A.I.R. semantically interoperable Resource Description
        Framework (RDF) triples through our guided four-step process
      </p>
    </div>

    <div class="row">
      <div
        v-for="card in cards"
        :key="card.id"
        class="col-xl-3 col-lg-3 col-md-6 mb-4 card-container"
      >
        <div
          :id="`${card.id}-card`"
          class="workflow-card"
          :class="{ disabled: isDisabled(card) }"
          :aria-disabled="isDisabled(card)"
          role="button"
          tabindex="0"
          @click="open(card)"
          @keyup.enter="open(card)"
          @mouseenter="showTooltip = card.id"
          @mouseleave="showTooltip = null"
          @focus="showTooltip = card.id"
          @blur="showTooltip = null"
        >
          <div
            class="step-icon"
            :class="card.iconClass"
          >
            <i
              class="fas"
              :class="card.titleIcon"
            />
          </div>
          <h3 class="step-title">
            {{ card.title }}
          </h3>
          <p class="step-description">
            {{ card.description }}
          </p>
          <ul class="step-features">
            <li
              v-for="feature in card.features"
              :key="feature"
            >
              <i class="fas fa-check" /> {{ feature }}
            </li>
          </ul>
        </div>
        <div
          v-if="showTooltip === card.id && isDisabled(card)"
          class="bootstrap-tooltip"
          role="tooltip"
        >
          First complete the data ingest step to proceed with this step.
        </div>
      </div>
    </div>

    <div class="cta-section">
      <h3><i class="fas fa-rocket text-primary" /> Ready to Get Started?</h3>
      <p class="mb-4">
        Begin transforming your data into semantically interoperable resources.
      </p>
      <RouterLink
        to="/ingest"
        class="btn btn-primary btn-lg"
      >
        <i class="fas fa-play" /> Start Data F.A.I.R.-ification Process
      </RouterLink>
    </div>
  </div>
</template>

<style scoped>
.hero-section::before {
  background-image: url('/static/image-flyover.svg');
}
.workflow-card {
  cursor: pointer;
}
/* Match legacy index.js behaviour: when Describe/Annotate are gated by a
   missing ingest, dim the card and block the pointer so users can't open the
   step. Legacy flyover-custom.css already styles .workflow-card.disabled with
   opacity 0.6 / cursor not-allowed — the SFC's scoped `cursor: pointer` rule
   above would otherwise out-cascade it, so restate it here. */
.workflow-card.disabled {
  cursor: not-allowed;
}
.workflow-card.disabled:hover {
  transform: none;
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
}

/* Custom tooltip for disabled cards */
.card-container {
  position: relative;
}

.bootstrap-tooltip {
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  margin-bottom: 0;
  padding: 0.25rem 0.5rem;
  background-color: rgba(0, 0, 0, 0.9);
  color: #fff;
  border-radius: 0.25rem;
  font-size: 0.875rem;
  white-space: nowrap;
  z-index: 1080;
  line-height: 1.5;
}

.bootstrap-tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border-width: 0.4rem 0.4rem 0;
  border-style: solid;
  border-color: rgba(0, 0, 0, 0.9) transparent transparent;
}
</style>

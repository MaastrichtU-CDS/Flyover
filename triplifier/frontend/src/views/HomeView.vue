<script setup>
import { useRouter } from 'vue-router'

const router = useRouter()

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

function open(card) {
  router.push(card.to)
}
</script>

<template>
  <div>
    <div class="hero-section">
      <h1 class="hero-title">Welcome to Flyover</h1>
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
        class="col-xl-3 col-lg-3 col-md-6 mb-4"
      >
        <div
          class="workflow-card"
          :id="`${card.id}-card`"
          @click="open(card)"
          role="button"
          tabindex="0"
          @keyup.enter="open(card)"
        >
          <div class="step-icon" :class="card.iconClass">
            <i class="fas" :class="card.titleIcon"></i>
          </div>
          <h3 class="step-title">{{ card.title }}</h3>
          <p class="step-description">{{ card.description }}</p>
          <ul class="step-features">
            <li v-for="feature in card.features" :key="feature">
              <i class="fas fa-check"></i> {{ feature }}
            </li>
          </ul>
        </div>
      </div>
    </div>

    <div class="cta-section">
      <h3><i class="fas fa-rocket text-primary"></i> Ready to Get Started?</h3>
      <p class="mb-4">
        Begin transforming your data into semantically interoperable resources.
      </p>
      <RouterLink to="/ingest" class="btn btn-primary btn-lg">
        <i class="fas fa-play"></i> Start Data F.A.I.R.-ification Process
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
</style>

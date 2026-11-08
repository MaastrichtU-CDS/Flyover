import { createRouter, createWebHistory } from 'vue-router'

// The home page offers the workflows. The RDF workflow's pages carry
// meta.workflow, and those that read or write the store also wait for it
// (see composables/useStoreGate.js); its mock-data and publish pages work
// without it.
const RDF = { workflow: 'rdf' }
const RDF_STORE = { workflow: 'rdf', needsStore: true }

const routes = [
  { path: '/', name: 'home', component: () => import('@/views/HomeView.vue') },
  { path: '/rdf', name: 'rdf', component: () => import('@/views/RdfLandingView.vue'), meta: RDF },
  { path: '/ingest', name: 'ingest', component: () => import('@/views/IngestView.vue'), meta: RDF_STORE },
  { path: '/describe', name: 'describe', component: () => import('@/views/DescribeLandingView.vue'), meta: RDF_STORE },
  { path: '/describe/variables', name: 'describe-variables', component: () => import('@/views/DescribeVariablesView.vue'), meta: RDF_STORE },
  { path: '/describe/variable-details', name: 'describe-variable-details', component: () => import('@/views/DescribeVariableDetailsView.vue'), meta: RDF_STORE },
  { path: '/annotate', name: 'annotate', component: () => import('@/views/AnnotationLandingView.vue'), meta: RDF_STORE },
  { path: '/annotate/review', name: 'annotate-review', component: () => import('@/views/AnnotationReviewView.vue'), meta: RDF_STORE },
  { path: '/annotate/verify', name: 'annotate-verify', component: () => import('@/views/AnnotationVerifyView.vue'), meta: RDF_STORE },
  { path: '/share', name: 'share', component: () => import('@/views/ShareLandingView.vue'), meta: RDF_STORE },
  { path: '/share/mock', name: 'share-mock', component: () => import('@/views/ShareMockView.vue'), meta: RDF },
  { path: '/share/publish', name: 'share-publish', component: () => import('@/views/SharePublishView.vue'), meta: RDF },
]

const router = createRouter({
  history: createWebHistory('/'),
  routes,
})

export default router

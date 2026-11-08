import { createRouter, createWebHistory } from 'vue-router'

// The pages that read or write the store: they wait for it (see
// composables/useStoreGate.js). Home and the mock-data and publish pages work
// without it.
const STORE = { needsStore: true }

const routes = [
  { path: '/', name: 'home', component: () => import('@/views/HomeView.vue') },
  { path: '/ingest', name: 'ingest', component: () => import('@/views/IngestView.vue'), meta: STORE },
  { path: '/describe', name: 'describe', component: () => import('@/views/DescribeLandingView.vue'), meta: STORE },
  { path: '/describe/variables', name: 'describe-variables', component: () => import('@/views/DescribeVariablesView.vue'), meta: STORE },
  { path: '/describe/variable-details', name: 'describe-variable-details', component: () => import('@/views/DescribeVariableDetailsView.vue'), meta: STORE },
  { path: '/annotate', name: 'annotate', component: () => import('@/views/AnnotationLandingView.vue'), meta: STORE },
  { path: '/annotate/review', name: 'annotate-review', component: () => import('@/views/AnnotationReviewView.vue'), meta: STORE },
  { path: '/annotate/verify', name: 'annotate-verify', component: () => import('@/views/AnnotationVerifyView.vue'), meta: STORE },
  { path: '/share', name: 'share', component: () => import('@/views/ShareLandingView.vue'), meta: STORE },
  { path: '/share/mock', name: 'share-mock', component: () => import('@/views/ShareMockView.vue') },
  { path: '/share/publish', name: 'share-publish', component: () => import('@/views/SharePublishView.vue') },
]

const router = createRouter({
  history: createWebHistory('/'),
  routes,
})

export default router

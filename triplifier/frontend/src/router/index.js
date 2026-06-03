import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/', name: 'home', component: () => import('@/views/HomeView.vue') },
  { path: '/ingest', name: 'ingest', component: () => import('@/views/IngestView.vue') },
  { path: '/describe', name: 'describe', component: () => import('@/views/DescribeLandingView.vue') },
  { path: '/describe/variables', name: 'describe-variables', component: () => import('@/views/DescribeVariablesView.vue') },
  { path: '/describe/variable-details', name: 'describe-variable-details', component: () => import('@/views/DescribeVariableDetailsView.vue') },
  { path: '/annotate', name: 'annotate', component: () => import('@/views/AnnotationLandingView.vue') },
  { path: '/annotate/review', name: 'annotate-review', component: () => import('@/views/AnnotationReviewView.vue') },
  { path: '/annotate/verify', name: 'annotate-verify', component: () => import('@/views/AnnotationVerifyView.vue') },
  { path: '/share', name: 'share', component: () => import('@/views/ShareLandingView.vue') },
  { path: '/share/mock', name: 'share-mock', component: () => import('@/views/ShareMockView.vue') },
  { path: '/share/publish', name: 'share-publish', component: () => import('@/views/SharePublishView.vue') },
]

const router = createRouter({
  history: createWebHistory('/'),
  routes,
})

export default router

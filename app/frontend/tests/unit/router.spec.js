import { describe, it, expect } from 'vitest'
import router from '@/router'
import { WORKFLOWS } from '@/lib/workflows'

// The route table says which workflow each page belongs to (meta.workflow)
// and which pages wait for the workflow's store (meta.needsStore). The page
// gate (useStoreGate), the pop-up that asks for the project and the question
// on leaving a workflow (App.vue) all go by it.

function pages(filter = () => true) {
  return router
    .getRoutes()
    .filter(filter)
    .map((route) => route.name)
    .sort()
}

describe('the route table', () => {
  it('keeps the home page outside every workflow', () => {
    expect(pages((route) => !route.meta.workflow)).toEqual(['home'])
  })

  it('puts every page of the RDF steps in the RDF workflow', () => {
    expect(pages((route) => route.meta.workflow === 'rdf')).toEqual([
      'annotate',
      'annotate-review',
      'annotate-verify',
      'describe',
      'describe-variable-details',
      'describe-variables',
      'ingest',
      'rdf',
      'share',
      'share-mock',
      'share-publish',
    ])
  })

  it('lets only the pages that read or write the store wait for it', () => {
    expect(pages((route) => route.meta.needsStore)).toEqual([
      'annotate',
      'annotate-review',
      'annotate-verify',
      'describe',
      'describe-variable-details',
      'describe-variables',
      'ingest',
      'share',
    ])
  })

  it('names only workflows the home page offers', () => {
    const offered = WORKFLOWS.map((workflow) => workflow.id)

    for (const route of router.getRoutes()) {
      if (route.meta.workflow) expect(offered).toContain(route.meta.workflow)
    }
  })

  it('opens each built workflow on a landing page of its own', () => {
    for (const workflow of WORKFLOWS.filter((w) => w.available)) {
      const landing = router.resolve(workflow.to)

      expect(landing.matched).toHaveLength(1)
      expect(landing.meta).toEqual({ workflow: workflow.id })
    }
  })
})

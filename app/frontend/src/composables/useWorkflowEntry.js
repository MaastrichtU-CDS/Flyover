import { useRouter } from 'vue-router'
import { useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

// Entering a workflow from the home page: its tile, the start button or its
// item in the navigation bar. A workflow under development says so. For one
// that is built, Flyover first asks for the project, on the home page; the
// pop-up goes on to the workflow's page once the tab has chosen (see
// ProjectPicker), and a cancel leaves the user on the home page.
export function useWorkflowEntry() {
  const router = useRouter()
  const project = useProjectStore()
  const workflows = useWorkflowsStore()

  function enter(workflow) {
    if (!workflow.available) {
      workflows.showSoon(workflow.id)
      return
    }
    if (project.workflow === workflow.id) {
      router.push(workflow.to)
      return
    }
    project.openPicker('enter', { workflow: workflow.id, target: workflow.to })
  }

  return { enter }
}

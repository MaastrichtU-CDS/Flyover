// The workflows Flyover offers on its home page, each with its own steps
// and its own store. Only RDF is built; OMOP and CSV say they are under
// development. A route belongs to a workflow through its meta.workflow.

export const WORKFLOWS = [
  {
    id: 'rdf',
    label: 'RDF',
    to: '/rdf',
    icon: 'fa-project-diagram',
    iconClass: 'share-icon',
    description:
      'Turn your data into F.A.I.R. RDF triples in an RDF store: ingest, describe, annotate and share it.',
    features: [
      'CSV, Excel and database input',
      'Variable descriptions',
      'Ontology annotations',
      'Sharing and DCAT-AP export',
    ],
    available: true,
  },
  {
    id: 'omop',
    label: 'OMOP',
    icon: 'fa-database',
    iconClass: 'describe-icon',
    description:
      "Bring your data into the OMOP Common Data Model, in the OMOP database your project's JSON-LD names.",
    features: ['OMOP CDM tables', 'Standard vocabularies', 'Database from your JSON-LD'],
    available: false,
  },
  {
    id: 'csv',
    label: 'CSV',
    icon: 'fa-file-csv',
    iconClass: 'annotate-icon',
    description:
      'Describe and annotate your data, and export it as a mapped CSV file, with no database at all.',
    features: ['No database needed', 'Mapped CSV export', 'Ontology terms kept with it'],
    available: false,
  },
]

export function workflowById(id) {
  return WORKFLOWS.find((workflow) => workflow.id === id) || null
}

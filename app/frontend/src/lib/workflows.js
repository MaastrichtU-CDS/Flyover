// The workflows Flyover offers on its home page, each with its own steps
// and its own store. Only RDF is built; OMOP and CSV say they are under
// development. A route belongs to a workflow through its meta.workflow.
//
// - `logo`: a third-party logo, in white, on the tile and in the navigation
//   bar: the owner's own white version, or the logo turned white
//   (`whiten`) where its owner allows other colours (see
//   app/backend/data_descriptor/static/logos/README.md for its source and
//   terms; W3C asks for the cube's alt text as given).
// - `icon`: the Font Awesome icon used where there is no logo.
// - `storeLabel`: what the workflow's store is called, where it has one.
// - `note`: small print on the tile, as lines of text and links.

export const WORKFLOWS = [
  {
    id: 'rdf',
    label: 'RDF',
    to: '/rdf',
    icon: 'fa-project-diagram',
    iconClass: 'share-icon',
    storeLabel: 'RDF store',
    logo: {
      src: '/static/logos/w3c-semantic-web-cube.svg',
      alt: 'W3C Semantic Web Logo',
      whiten: true,
    },
    description:
      'Transform your data to F.A.I.R. resources using the flexibility of RDF, coupled with an RDF store.',
    features: ['Flexible vocabularies', 'Flexible data schemas', 'Enables complex reasoning'],
    note: [
      [
        'RDF is a Semantic Web technology of the ',
        { text: 'World Wide Web Consortium (W3C)', href: 'https://www.w3.org/' },
        '.',
      ],
      ['Flyover is not affiliated with W3C.'],
    ],
    available: true,
  },
  {
    id: 'omop',
    label: 'OMOP',
    icon: 'fa-database',
    iconClass: 'describe-icon',
    storeLabel: 'OMOP database',
    logo: { src: '/static/logos/ohdsi-logo-only-white.png', alt: 'OHDSI logo' },
    description:
      'Transform your data into OMOP CDM-conformant schemas, coupled with an OMOP database.',
    features: [
      'Standardised vocabularies',
      'Standardised data schema',
      'OHDSI tools and network studies',
    ],
    note: [
      [
        'The OMOP common data model is maintained by ',
        { text: 'OHDSI', href: 'https://www.ohdsi.org/' },
        '.',
      ],
      ['Flyover is not affiliated with OHDSI.'],
    ],
    available: false,
  },
  {
    id: 'csv',
    label: 'CSV',
    icon: 'fa-file-csv',
    iconClass: 'annotate-icon',
    description:
      'Describe and transform your data to a standardised terminology with the simplicity of CSV files.',
    features: ['No database needed', 'Works with any analysis tool', 'Portable and easily shareable files'],
    note: [['CSV files are simple text files, usable as spreadsheets.']],
    available: false,
  },
]

export function workflowById(id) {
  return WORKFLOWS.find((workflow) => workflow.id === id) || null
}

/**
 * Annotation Review App - Vue.js 3 Component
 *
 * Replaces the AnnotationReviewPage module with a reactive Vue 3 application
 * using the Options API. Handles loading semantic map data from IndexedDB,
 * validating against RDF store databases, and rendering the annotation review
 * interface with per-database pagination.
 *
 * Uses [[ ]] delimiters to avoid conflicts with Jinja2 {{ }} syntax.
 * Mounts to the #annotation-review-app element in the DOM.
 *
 * @requires Vue 3 CDN (loaded in HTML template)
 * @requires FlyoverDB (from db-utils.js)
 */

/** @const {number} Number of variable cards displayed per page */
const PAGE_SIZE = 10;

const AnnotationReviewApp = Vue.createApp({
    delimiters: ['[[', ']]'],

    /**
     * Component template containing the loading spinner, no-data warning,
     * database sections with variable cards, pagination, and action buttons.
     */
    template: `
    <div>
        <!-- Loading spinner -->
        <div v-if="loading" class="loading-spinner">
            <i class="fas fa-spinner fa-spin"></i>
            <p>Loading semantic map from browser storage...</p>
        </div>

        <!-- No data warning -->
        <div v-else-if="noDataMessage" class="alert alert-warning">
            <i class="fas fa-exclamation-triangle"></i>
            <span v-html="noDataMessage"></span>
        </div>

        <!-- Main content -->
        <div v-else>
            <!-- Database info section - only shows issues -->
            <div v-if="nonMatchingJsonld.length > 0 || nonMatchingRdfStore.length > 0" class="mb-4">
                <div v-if="nonMatchingJsonld.length > 0"
                     class="alert alert-warning alert-compact">
                    <i class="fas fa-exclamation-triangle"></i>
                    <strong>Will not be annotated</strong> (not in the RDF store):
                    [[ nonMatchingJsonld.join(', ') ]]
                </div>
                <div v-if="nonMatchingRdfStore.length > 0"
                     class="alert alert-info alert-compact">
                    <i class="fas fa-info-circle"></i>
                    <strong>Other data in RDF store</strong> (no mapping provided):
                    [[ nonMatchingRdfStore.join(', ') ]]
                </div>
            </div>

            <form class="form-horizontal">
                <div v-for="(dbData, dbName) in annotatedTableVariables" :key="dbName">
                    <h2 class="database-heading">
                        <i class="fas fa-database"></i> [[ dbData.rdfStoreName ]]
                    </h2>
                    <button class="toggle-button" type="button"
                            :class="{ open: expandedDatabases[dbData.rdfStoreName] }"
                            @click="toggleDatabase(dbData.rdfStoreName)">
                        <span class="toggle-text">
                            [[ expandedDatabases[dbData.rdfStoreName] ? 'Show less' : 'Show more' ]]
                        </span>
                        <i class="fas" :class="expandedDatabases[dbData.rdfStoreName] ? 'fa-chevron-down' : 'fa-chevron-up'"></i>
                    </button>

                    <div class="content"
                         :class="{ active: expandedDatabases[dbData.rdfStoreName],
                                   hidden: !expandedDatabases[dbData.rdfStoreName] }">
                        <div class="database-summary">
                            <strong>Database Summary:</strong>
                            [[ Object.keys(dbData.variables).length ]] variable(s) ready for annotation
                        </div>
                        <div class="variables-container">
                            <div v-for="(varInfo, varName) in currentPageVariables(dbData.rdfStoreName, dbData.variables)"
                                 :key="varName" class="variable-card">
                                <div class="variable-header">
                                    <div class="variable-name">[[ varName ]]</div>
                                    <div class="status-badge"
                                         :class="varInfo.local_definition ? 'status-annotated' : 'status-unannotated'">
                                        <i class="fas"
                                           :class="varInfo.local_definition ? 'fa-check-circle' : 'fa-exclamation-triangle'"></i>
                                        [[ varInfo.local_definition ? 'Described' : 'Undescribed' ]]
                                    </div>
                                </div>
                                <div class="variable-details">
                                    <div class="detail-row">
                                        <div class="detail-label">Local Definition:</div>
                                        <div class="detail-value">
                                            [[ varInfo.local_definition || 'Not specified' ]]
                                        </div>
                                    </div>
                                    <div class="detail-row">
                                        <div class="detail-label">Predicate:</div>
                                        <div class="detail-value">
                                            <code>[[ varInfo.predicate || '' ]]</code>
                                        </div>
                                    </div>
                                    <div class="detail-row">
                                        <div class="detail-label">Class:</div>
                                        <div class="detail-value">
                                            <code>[[ varInfo.class || '' ]]</code>
                                        </div>
                                    </div>
                                    <div v-if="varInfo.data_type" class="detail-row">
                                        <div class="detail-label">Data Type:</div>
                                        <div class="detail-value">[[ varInfo.data_type ]]</div>
                                    </div>

                                    <!-- Value mapping -->
                                    <div v-if="hasValueMapping(varInfo)" class="detail-row">
                                        <div class="detail-label">Value Mapping:</div>
                                        <div class="detail-value">
                                            <div class="value-mapping-section">
                                                <div v-for="(termInfo, termKey) in varInfo.value_mapping.terms"
                                                     :key="termKey" class="value-mapping-item">
                                                    <span class="term-name">[[ toTitleCase(termKey) ]]</span>
                                                    <span :class="termInfo.local_term === '' ? 'local-value empty-value' : 'local-value'">
                                                        [[ termInfo.local_term === '' ? '(empty)' : termInfo.local_term ]]
                                                    </span>
                                                    <span class="target-class">
                                                        <code>[[ termInfo.target_class || '' ]]</code>
                                                    </span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Pagination controls -->
                        <div v-if="totalPages(dbData.rdfStoreName, dbData.variables) > 1"
                             class="pagination-controls">
                            <button type="button"
                                    :disabled="(databasePages[dbData.rdfStoreName] || 1) <= 1"
                                    @click="changePage(dbData.rdfStoreName, -1)">&#x2190;</button>
                            <span class="page-indicator">
                                Page <span class="current-page">[[ databasePages[dbData.rdfStoreName] || 1 ]]</span>
                                of <span class="total-pages">[[ totalPages(dbData.rdfStoreName, dbData.variables) ]]</span>
                            </span>
                            <button type="button"
                                    :disabled="(databasePages[dbData.rdfStoreName] || 1) >= totalPages(dbData.rdfStoreName, dbData.variables)"
                                    @click="changePage(dbData.rdfStoreName, 1)">&#x2192;</button>
                        </div>
                    </div>
                    <hr>
                </div>
            </form>

            <button id="startAnnotation" class="btn btn-primary"
                    :disabled="annotationProcessing"
                    @click="startAnnotationProcess">
                <i class="fas" :class="annotationProcessing ? 'fa-spinner fa-spin' : 'fa-play'"></i>
                [[ annotationButtonText ]]
            </button>
            <a href="/describe_variable_details" class="btn btn-light">
                <i class="fas fa-backward"></i> Back to Describe Variable Details
            </a>
            <br>

            <!-- Info alert box -->
            <div class="mt-4">
                <div class="alert alert-info-highlight py-2">
                    <i class="fas fa-info-circle"></i>
                    <strong>Annotating your data</strong><br>
                    <div class="mt-1 ms-4">
                        <div class="mt-1">
                            <p class="mb-1">
                                Please consider that annotation data can always be adapted without having to re-upload your data.<br>
                                You can do this by removing the annotation graph in the RDF store interface and simply
                                redoing the annotation process.
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    `,

    /**
     * Reactive state for the annotation review page.
     * @returns {Object} Initial component state
     */
    data() {
        return {
            /** @type {Object|null} Raw semantic map data from IndexedDB */
            semanticMapData: null,
            /** @type {string[]} Database names found in the RDF store */
            rdfStoreDatabases: [],
            /** @type {Object<string, number>} Current page number per database */
            databasePages: {},
            /** @type {boolean} Whether the page is in the loading state */
            loading: true,
            /** @type {string} Warning message shown when data cannot be loaded (supports HTML) */
            noDataMessage: '',
            /** @type {Object<string, boolean>} Toggle state per database section */
            expandedDatabases: {},
            /** @type {boolean} Whether the annotation submit is in progress */
            annotationProcessing: false,
            /** @type {string} Current label for the annotation button */
            annotationButtonText: 'Start Annotation Process',
            /**
             * Processed table variables keyed by table name.
             * Each value contains { variables, rdfStoreName }.
             * @type {Object<string, { variables: Object, rdfStoreName: string }>}
             */
            annotatedTableVariables: {},
            /** @type {string[]} JSON-LD tables that have no match in the RDF store */
            nonMatchingJsonld: [],
            /** @type {string[]} RDF store databases that have no match in the semantic map */
            nonMatchingRdfStore: []
        };
    },

    methods: {
        /**
         * Load the semantic map from IndexedDB and populate the page.
         * This is the main initialization method called on mount.
         */
        async loadSemanticMapFromIndexedDB() {
            try {
                console.log('Starting loadSemanticMapFromIndexedDB...');

                if (typeof FlyoverDB === 'undefined') {
                    console.error('FlyoverDB is undefined');
                    this.showNoDataWarning('Browser storage not available. Please ensure JavaScript is enabled.');
                    return;
                }

                if (!FlyoverDB.isSupported()) {
                    console.error('IndexedDB not supported');
                    this.showNoDataWarning('Your browser does not support IndexedDB. Please use a modern browser.');
                    return;
                }

                console.log('Initializing FlyoverDB...');
                await FlyoverDB.initDB();

                // First, try to fetch databases from RDF store
                console.log('Fetching RDF store databases...');
                let hasRdfStoreDatabases = await this.fetchAndStoreRdfStoreDatabases();

                // If API call failed, try loading from IndexedDB
                if (!hasRdfStoreDatabases) {
                    console.log('API call failed, trying IndexedDB...');
                    hasRdfStoreDatabases = await this.loadRdfStoreDatabasesFromIndexedDB();
                }

                if (!hasRdfStoreDatabases) {
                    console.warn('No RDF store databases available');
                    this.showNoDataWarning('No databases found in the data store. Please complete the Ingest step first.');
                    return;
                }

                console.log('RDF store databases loaded:', this.rdfStoreDatabases);

                // Load semantic map
                console.log('Loading semantic map from IndexedDB...');
                const result = await FlyoverDB.getData('metadata', 'semantic_map');

                if (!result || !result.data) {
                    console.log('No semantic map found in IndexedDB');
                    this.showNoDataWarning();
                    return;
                }

                this.semanticMapData = result.data;
                console.log('Loaded semantic map from IndexedDB:', this.semanticMapData);

                // Process and render the annotation data
                this.processAnnotationData(this.semanticMapData);

            } catch (error) {
                console.error('Error loading semantic map:', error);
                this.showNoDataWarning('An error occurred while loading: ' + error.message);
            }
        },

        /**
         * Fetch databases from the RDF store API and persist to IndexedDB.
         * @returns {Promise<boolean>} True if databases were fetched successfully
         */
        async fetchAndStoreRdfStoreDatabases() {
            try {
                const response = await fetch('/api/rdf-store-databases');
                const data = await response.json();

                if (data.success && data.databases && data.databases.length > 0) {
                    this.rdfStoreDatabases = data.databases;
                    console.log('Fetched RDF store databases:', this.rdfStoreDatabases);

                    await FlyoverDB.saveData('metadata', {
                        key: 'rdf_store_databases',
                        data: this.rdfStoreDatabases,
                        timestamp: new Date().toISOString()
                    });

                    return true;
                } else {
                    console.warn('No databases found in RDF store:', data.message);
                    return false;
                }
            } catch (error) {
                console.error('Error fetching RDF store databases:', error);
                return false;
            }
        },

        /**
         * Load RDF store databases from IndexedDB (fallback when the API fails).
         * @returns {Promise<boolean>} True if databases were loaded from cache
         */
        async loadRdfStoreDatabasesFromIndexedDB() {
            try {
                const result = await FlyoverDB.getData('metadata', 'rdf_store_databases');
                if (result && result.data && result.data.length > 0) {
                    this.rdfStoreDatabases = result.data;
                    console.log('Loaded RDF store databases from IndexedDB:', this.rdfStoreDatabases);
                    return true;
                }
            } catch (error) {
                console.error('Error loading RDF store databases from IndexedDB:', error);
            }
            return false;
        },

        /**
         * Check if a database name matches any RDF store database.
         * Handles cases like "file.csv" matching "file" or vice versa.
         * @param {string} mapDbName - Database name from the semantic map
         * @returns {string|null} The matching RDF store database name, or null
         */
        findMatchingDatabase(mapDbName) {
            if (!mapDbName || mapDbName === '') return this.rdfStoreDatabases[0] || null;

            for (const db of this.rdfStoreDatabases) {
                if (db === mapDbName) return db;

                const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
                const dbNoExt = db.endsWith('.csv') ? db.slice(0, -4) : db;
                if (mapNoExt === dbNoExt) return db;
            }
            return null;
        },

        /**
         * Extract table names from JSON-LD structure.
         * Tables are the actual data sources that match with the RDF store.
         * @param {Object} data - The semantic map data
         * @returns {string[]} List of table names
         */
        extractJsonLdTables(data) {
            const tables = [];
            if (data.databases) {
                for (const [dbKey, dbData] of Object.entries(data.databases)) {
                    if (dbData.tables) {
                        for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                            const tableName = (typeof tableData === 'object' && tableData.sourceFile)
                                ? tableData.sourceFile
                                : tableKey;
                            tables.push(tableName);
                        }
                    }
                }
            } else if (data.database_name) {
                tables.push(data.database_name);
            }
            return tables;
        },

        /**
         * Transform JSON-LD structure to variable info organized by table.
         * Only includes tables that have a match in the RDF store.
         * @param {Object} data - The semantic map data
         * @param {Array<{jsonld: string, rdf_store: string}>} matchingTables - Matched table pairs
         * @returns {Object} Variable info keyed by table name
         */
        transformJsonLdToVariableInfoByTable(data, matchingTables) {
            const result = {};
            const schemaVariables = data.schema?.variables || {};
            const databases = data.databases || {};

            const tableToRdfStore = {};
            for (const match of matchingTables) {
                tableToRdfStore[match.jsonld] = match.rdf_store;
            }

            for (const [dbKey, dbData] of Object.entries(databases)) {
                if (!dbData.tables) continue;

                for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                    const tableName = (tableData.sourceFile) ? tableData.sourceFile : tableKey;

                    if (!tableToRdfStore[tableName]) continue;

                    const rdfStoreName = tableToRdfStore[tableName];
                    const variableInfo = {};

                    if (!tableData.columns) continue;

                    for (const [colKey, colData] of Object.entries(tableData.columns)) {
                        if (!colData.localColumn) continue;
                        if (!colData.mapsTo) continue;

                        const varName = colData.mapsTo.split('/').pop();
                        if (!varName) continue;

                        const varSchema = schemaVariables[varName] || {};

                        const localCols = Array.isArray(colData.localColumn)
                            ? colData.localColumn
                            : [colData.localColumn];
                        const filteredCols = localCols.filter(c => c);

                        if (filteredCols.length === 0) continue;

                        const localDef = filteredCols.join(', ');
                        const localMappings = colData.localMappings || {};

                        variableInfo[varName] = {
                            predicate: varSchema.predicate || null,
                            class: varSchema.class || null,
                            data_type: varSchema.dataType || null,
                            local_definition: localDef,
                            value_mapping: null
                        };

                        // Add value mapping if present - only include terms with local mappings
                        if (varSchema.valueMapping && varSchema.valueMapping.terms) {
                            const terms = {};
                            let hasAnyLocalTerms = false;

                            for (const [termKey, termData] of Object.entries(varSchema.valueMapping.terms)) {
                                const localTermValue = localMappings[termKey];

                                if (localTermValue === undefined || localTermValue === null) continue;

                                let localTerm;
                                if (Array.isArray(localTermValue)) {
                                    localTerm = localTermValue.join(', ');
                                } else {
                                    localTerm = localTermValue;
                                }

                                terms[termKey] = {
                                    target_class: termData.targetClass || null,
                                    local_term: localTerm
                                };
                                hasAnyLocalTerms = true;
                            }

                            if (hasAnyLocalTerms) {
                                variableInfo[varName].value_mapping = { terms };
                            }
                        }
                    }

                    if (Object.keys(variableInfo).length > 0) {
                        result[tableName] = {
                            variableInfo: variableInfo,
                            rdfStoreName: rdfStoreName
                        };
                    }
                }
            }

            return result;
        },

        /**
         * Process semantic map data: compute comparisons, filter annotated variables,
         * and populate reactive state. Replaces renderAnnotationData from the original module.
         * @param {Object} data - The semantic map data
         */
        processAnnotationData(data) {
            try {
                console.log('processAnnotationData called with:', data);

                const jsonldTables = this.extractJsonLdTables(data);
                console.log('Extracted JSON-LD tables:', jsonldTables);

                // Compute database comparison
                const matching = [];
                const nonMatchingJsonld = [];
                const nonMatchingRdfStore = [...this.rdfStoreDatabases];

                for (const jsonldTable of jsonldTables) {
                    const match = this.findMatchingDatabase(jsonldTable);
                    if (match) {
                        matching.push({ jsonld: jsonldTable, rdf_store: match });
                        const idx = nonMatchingRdfStore.indexOf(match);
                        if (idx > -1) nonMatchingRdfStore.splice(idx, 1);
                    } else {
                        nonMatchingJsonld.push(jsonldTable);
                    }
                }

                this.nonMatchingJsonld = nonMatchingJsonld;
                this.nonMatchingRdfStore = nonMatchingRdfStore;

                console.log('Database comparison:', { matching, nonMatchingJsonld, nonMatchingRdfStore });

                if (matching.length === 0) {
                    this.showNoDataWarning(
                        '<strong>Cannot proceed with annotation</strong><br>' +
                        'None of the data sources in the semantic map match data in the RDF store.<br><br>' +
                        '<strong>Data sources in semantic map:</strong> ' + this.escapeHtml(jsonldTables.join(', ') || 'None') + '<br>' +
                        '<strong>Data in RDF store:</strong> ' + this.escapeHtml(this.rdfStoreDatabases.join(', ') || 'None')
                    );
                    return;
                }

                // Transform JSON-LD to variable info organized by table
                const tableVariables = this.transformJsonLdToVariableInfoByTable(data, matching);
                console.log('Table variables:', tableVariables);

                if (Object.keys(tableVariables).length === 0) {
                    this.showNoDataWarning('No variables with local column definitions found in the semantic map.');
                    return;
                }

                // Filter to only annotated variables (have predicate, class, and local_definition)
                const annotatedTableVariables = {};
                let totalAnnotatedVars = 0;

                for (const [tableName, tableData] of Object.entries(tableVariables)) {
                    const { variableInfo, rdfStoreName } = tableData;
                    const annotatedVars = {};

                    for (const [varName, varData] of Object.entries(variableInfo)) {
                        if (!varData.predicate || !varData.class || !varData.local_definition) {
                            continue;
                        }
                        annotatedVars[varName] = varData;
                    }

                    console.log(`Table ${tableName}: ${Object.keys(annotatedVars).length} annotated`);

                    if (Object.keys(annotatedVars).length > 0) {
                        annotatedTableVariables[tableName] = {
                            variables: annotatedVars,
                            rdfStoreName: rdfStoreName
                        };
                        totalAnnotatedVars += Object.keys(annotatedVars).length;
                    }
                }

                console.log('Total annotated vars:', totalAnnotatedVars);

                if (totalAnnotatedVars === 0) {
                    this.showNoDataWarning(
                        'No variables are ready for annotation. ' +
                        'Please ensure variables have local definitions, predicates, and classes.'
                    );
                    return;
                }

                // Initialize pagination and toggle state for each database
                for (const [, tableData] of Object.entries(annotatedTableVariables)) {
                    this.databasePages[tableData.rdfStoreName] = 1;
                    this.expandedDatabases[tableData.rdfStoreName] = false;
                }

                this.annotatedTableVariables = annotatedTableVariables;
                this.loading = false;

            } catch (error) {
                console.error('Error in processAnnotationData:', error);
                this.showNoDataWarning('An error occurred while processing the semantic map: ' + error.message);
            }
        },

        /**
         * Set the no-data warning message and exit the loading state.
         * @param {string} [message] - Custom warning message (supports HTML via v-html)
         */
        showNoDataWarning(message) {
            this.loading = false;
            if (message) {
                this.noDataMessage =
                    '<i class="fas fa-exclamation-triangle"></i> ' +
                    '<strong>Cannot proceed with annotation</strong><br>' +
                    message +
                    '<br><br>' +
                    '<a href="/describe_landing" class="btn btn-primary"><i class="fas fa-backward"></i> Go to Describe</a> ' +
                    '<a href="/ingest" class="btn btn-secondary"><i class="fas fa-fast-backward"></i> Go to Ingest</a> ' +
                    '<a href="/" class="btn btn-light"><i class="fas fa-home"></i> Return to Home</a>';
            } else {
                this.noDataMessage =
                    '<i class="fas fa-exclamation-triangle"></i> ' +
                    '<strong>No semantic map found</strong><br>' +
                    'Please ensure you have completed the describe workflow and have a semantic map in your browser storage.' +
                    '<br><br>' +
                    '<a href="/describe_landing" class="btn btn-primary">Go to Describe</a> ' +
                    '<a href="/" class="btn btn-light">Return to Home</a>';
            }
        },

        /**
         * Return the variables for the current page of a given database section.
         * @param {string} dbName - The RDF store database name
         * @param {Object} variables - All variables for this database
         * @returns {Object} A subset of variables for the current page
         */
        currentPageVariables(dbName, variables) {
            const page = this.databasePages[dbName] || 1;
            const entries = Object.entries(variables);
            const startIdx = (page - 1) * PAGE_SIZE;
            const endIdx = Math.min(startIdx + PAGE_SIZE, entries.length);
            const pageEntries = entries.slice(startIdx, endIdx);

            const result = {};
            for (const [key, value] of pageEntries) {
                result[key] = value;
            }
            return result;
        },

        /**
         * Calculate total number of pages for a database section.
         * @param {string} dbName - The RDF store database name
         * @param {Object} variables - All variables for this database
         * @returns {number} Total page count
         */
        totalPages(dbName, variables) {
            return Math.ceil(Object.keys(variables).length / PAGE_SIZE);
        },

        /**
         * Navigate to a different page for a specific database section.
         * @param {string} database - The RDF store database name
         * @param {number} direction - Page offset (-1 for previous, 1 for next)
         */
        changePage(database, direction) {
            const currentPage = this.databasePages[database] || 1;
            const newPage = currentPage + direction;

            // Find the matching table data for this database
            for (const tableData of Object.values(this.annotatedTableVariables)) {
                if (tableData.rdfStoreName === database) {
                    const pages = this.totalPages(database, tableData.variables);
                    if (newPage >= 1 && newPage <= pages) {
                        this.databasePages[database] = newPage;
                    }
                    break;
                }
            }
        },

        /**
         * Toggle the expanded/collapsed state of a database section.
         * @param {string} dbName - The RDF store database name
         */
        toggleDatabase(dbName) {
            this.expandedDatabases[dbName] = !this.expandedDatabases[dbName];
        },

        /**
         * Check whether a variable has a non-empty value mapping.
         * @param {Object} varInfo - The variable information object
         * @returns {boolean} True if value mapping with terms exists
         */
        hasValueMapping(varInfo) {
            return varInfo.value_mapping &&
                   varInfo.value_mapping.terms &&
                   Object.keys(varInfo.value_mapping.terms).length > 0;
        },

        /**
         * Convert an underscore-separated key to Title Case.
         * @param {string} key - The term key (e.g. "some_term_name")
         * @returns {string} Title-cased label (e.g. "Some Term Name")
         */
        toTitleCase(key) {
            return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        },

        /**
         * Escape HTML special characters to prevent XSS.
         * Used only for content rendered via v-html.
         * @param {string} text - Raw text to escape
         * @returns {string} HTML-escaped text
         */
        escapeHtml(text) {
            if (typeof StatusMessages !== 'undefined' && StatusMessages.escapeHtml) {
                return StatusMessages.escapeHtml(text);
            }
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        },

        /**
         * Submit the semantic map to the backend and start the annotation process.
         * POSTs to /submit-indexeddb-semantic-map then /start-annotation.
         */
        async startAnnotationProcess() {
            if (!this.semanticMapData) {
                alert('No semantic map data available. Please reload the page.');
                return;
            }

            this.annotationProcessing = true;
            this.annotationButtonText = 'Submitting semantic map...';

            try {
                // Submit the semantic map to the backend
                const submitResponse = await fetch('/submit-indexeddb-semantic-map', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(this.semanticMapData)
                });

                const submitData = await submitResponse.json();

                if (!submitResponse.ok || !submitData.success) {
                    let errorMessage = submitData.error || 'Failed to submit semantic map.';
                    if (submitData.validation_errors && submitData.validation_errors.length > 0) {
                        errorMessage += '\n\nValidation errors:';
                        submitData.validation_errors.forEach(function(err) {
                            errorMessage += '\n- ' + err.message;
                        });
                    }
                    alert(errorMessage);
                    this.annotationProcessing = false;
                    this.annotationButtonText = 'Start Annotation Process';
                    return;
                }

                // Start the annotation process
                this.annotationButtonText = 'Processing Annotations...';

                const annotationResponse = await fetch('/start-annotation', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });

                const annotationData = await annotationResponse.json();

                if (annotationData.success) {
                    window.location.href = '/annotation-verify';
                } else {
                    alert('Error: ' + annotationData.error);
                    this.annotationProcessing = false;
                    this.annotationButtonText = 'Start Annotation Process';
                }
            } catch (error) {
                console.error('Error:', error);
                alert('An error occurred while processing. Please try again.');
                this.annotationProcessing = false;
                this.annotationButtonText = 'Start Annotation Process';
            }
        }
    },

    /**
     * Lifecycle hook: begin loading semantic map data once the component is mounted.
     */
    mounted() {
        this.loadSemanticMapFromIndexedDB();
    }
});

// Export for global access
window.AnnotationReviewApp = AnnotationReviewApp;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AnnotationReviewApp;
}

/**
 * Annotation Review Page Module
 * Handles loading semantic map data from IndexedDB, validating against GraphDB databases,
 * and rendering the annotation review interface with pagination.
 */

const AnnotationReviewPage = {
    // Configuration
    PAGE_SIZE: 10,

    // State
    semanticMapData: null,
    graphDbDatabases: [],
    databasePages: {},
    databaseVariables: {},

    /**
     * Initialize the annotation review page
     */
    init: function() {
        this.loadSemanticMapFromIndexedDB();
    },

    /**
     * Fetch databases from GraphDB and store in IndexedDB
     */
    fetchAndStoreGraphDbDatabases: async function() {
        try {
            const response = await fetch('/api/graphdb-databases');
            const data = await response.json();

            if (data.success && data.databases && data.databases.length > 0) {
                this.graphDbDatabases = data.databases;
                console.log('Fetched GraphDB databases:', this.graphDbDatabases);

                // Store in IndexedDB for use across pages
                await FlyoverDB.saveData('metadata', {
                    key: 'graphdb_databases',
                    data: this.graphDbDatabases,
                    timestamp: new Date().toISOString()
                });

                return true;
            } else {
                console.warn('No databases found in GraphDB:', data.message);
                return false;
            }
        } catch (error) {
            console.error('Error fetching GraphDB databases:', error);
            return false;
        }
    },

    /**
     * Load GraphDB databases from IndexedDB (fallback if API call fails)
     */
    loadGraphDbDatabasesFromIndexedDB: async function() {
        try {
            const result = await FlyoverDB.getData('metadata', 'graphdb_databases');
            if (result && result.data && result.data.length > 0) {
                this.graphDbDatabases = result.data;
                console.log('Loaded GraphDB databases from IndexedDB:', this.graphDbDatabases);
                return true;
            }
        } catch (error) {
            console.error('Error loading GraphDB databases from IndexedDB:', error);
        }
        return false;
    },

    /**
     * Check if a database name matches any GraphDB database
     * Handles cases like "file.csv" matching "file" or vice versa
     */
    findMatchingDatabase: function(mapDbName) {
        if (!mapDbName || mapDbName === '') return this.graphDbDatabases[0] || null;

        for (const db of this.graphDbDatabases) {
            if (db === mapDbName) return db;

            // Try matching without .csv extension
            const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
            const dbNoExt = db.endsWith('.csv') ? db.slice(0, -4) : db;
            if (mapNoExt === dbNoExt) return db;
        }
        return null;
    },

    /**
     * Load semantic map from IndexedDB and render the page
     */
    loadSemanticMapFromIndexedDB: async function() {
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

            // First, try to fetch databases from GraphDB
            console.log('Fetching GraphDB databases...');
            let hasGraphDbDatabases = await this.fetchAndStoreGraphDbDatabases();

            // If API call failed, try loading from IndexedDB
            if (!hasGraphDbDatabases) {
                console.log('API call failed, trying IndexedDB...');
                hasGraphDbDatabases = await this.loadGraphDbDatabasesFromIndexedDB();
            }

            if (!hasGraphDbDatabases) {
                console.warn('No GraphDB databases available');
                this.showNoDataWarning('No databases found in the data store. Please complete the Ingest step first.');
                return;
            }

            console.log('GraphDB databases loaded:', this.graphDbDatabases);

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

            // Render the annotation data with GraphDB database validation
            this.renderAnnotationData(this.semanticMapData);

        } catch (error) {
            console.error('Error loading semantic map:', error);
            this.showNoDataWarning('An error occurred while loading: ' + error.message);
        }
    },

    /**
     * Show the no data warning and hide loading state
     */
    showNoDataWarning: function(message) {
        document.getElementById('loadingState').style.display = 'none';
        document.getElementById('noDataWarning').style.display = 'block';
        document.getElementById('annotationContent').style.display = 'none';

        // Update message if provided
        if (message) {
            const warningDiv = document.getElementById('noDataWarning');
            warningDiv.innerHTML = `
                <i class="fas fa-exclamation-triangle"></i>
                <strong>Cannot proceed with annotation</strong><br>
                ${message}
                <br><br>
                <a href="/describe_landing" class="btn btn-primary"> <i class="fas fa-backward"></i> Go to Describe</a>
                <a href="/ingest" class="btn btn-secondary"><i class="fas fa-fast-backward"></i> Go to Ingest</a>
                <a href="/ingest" class="btn btn-secondary"><i class="fas fa-play">Go to Share</a>
                <a href="/" class="btn btn-light"><i class="fas fa-home"></i> Return to Home</a>
            `;
        }
    },

    /**
     * Extract table names from JSON-LD structure
     * Tables are the actual data sources that match with GraphDB
     */
    extractJsonLdTables: function(data) {
        const tables = [];
        if (data.databases) {
            for (const [dbKey, dbData] of Object.entries(data.databases)) {
                if (dbData.tables) {
                    for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                        // Use sourceFile if available, otherwise use the table key
                        const tableName = (typeof tableData === 'object' && tableData.sourceFile)
                            ? tableData.sourceFile
                            : tableKey;
                        tables.push(tableName);
                    }
                }
            }
        } else if (data.database_name) {
            // Fallback for flat structure
            tables.push(data.database_name);
        }
        return tables;
    },

    /**
     * Generate database comparison and populate the info section
     */
    populateDatabaseComparisonSection: function(jsonldTables) {
        // Calculate matches
        const matching = [];
        const nonMatchingJsonld = [];
        const nonMatchingGraphDb = [...this.graphDbDatabases];

        for (const jsonldTable of jsonldTables) {
            const match = this.findMatchingDatabase(jsonldTable);
            if (match) {
                matching.push({ jsonld: jsonldTable, graphdb: match });
                const idx = nonMatchingGraphDb.indexOf(match);
                if (idx > -1) nonMatchingGraphDb.splice(idx, 1);
            } else {
                nonMatchingJsonld.push(jsonldTable);
            }
        }

        // Only show info section if there are non-matching databases
        const infoSection = document.getElementById('databaseInfoSection');

        if (nonMatchingJsonld.length > 0 || nonMatchingGraphDb.length > 0) {
            let infoHtml = '';

            if (nonMatchingJsonld.length > 0) {
                infoHtml += `<div class="alert alert-warning" style="font-size: 0.9em;">
                    <i class="fas fa-exclamation-triangle"></i>
                    <strong>Will not be annotated</strong> (not in GraphDB): ${nonMatchingJsonld.map(t => this.escapeHtml(t)).join(', ')}
                </div>`;
            }

            if (nonMatchingGraphDb.length > 0) {
                infoHtml += `<div class="alert alert-info" style="font-size: 0.9em;">
                    <i class="fas fa-info-circle"></i>
                    <strong>Other data in GraphDB</strong> (no mapping provided): ${nonMatchingGraphDb.map(t => this.escapeHtml(t)).join(', ')}
                </div>`;
            }

            infoSection.innerHTML = infoHtml;
            infoSection.style.display = 'block';
        } else {
            infoSection.style.display = 'none';
        }

        return {
            matching: matching,
            nonMatchingJsonld: nonMatchingJsonld,
            nonMatchingGraphDb: nonMatchingGraphDb,
            hasMatches: matching.length > 0
        };
    },

    /**
     * Render the annotation data from the semantic map
     */
    renderAnnotationData: function(data) {
        try {
            console.log('renderAnnotationData called with:', data);

            // Extract all tables from JSON-LD (these are the actual data sources)
            const jsonldTables = this.extractJsonLdTables(data);
            console.log('Extracted JSON-LD tables:', jsonldTables);

            // Populate database comparison section
            const comparison = this.populateDatabaseComparisonSection(jsonldTables);
            console.log('Database comparison:', comparison);

            if (!comparison.hasMatches) {
                this.showNoDataWarning(
                    `None of the data sources in the semantic map match data in GraphDB.<br><br>` +
                    `<strong>Data sources in semantic map:</strong> ${jsonldTables.join(', ') || 'None'}<br>` +
                    `<strong>Data in GraphDB:</strong> ${this.graphDbDatabases.join(', ') || 'None'}`
                );
                return;
            }

            // Transform JSON-LD to get variable info organized by table
            const tableVariables = this.transformJsonLdToVariableInfoByTable(data, comparison.matching);
            console.log('Table variables:', tableVariables);

            // Check if we have any tables with variables
            if (Object.keys(tableVariables).length === 0) {
                this.showNoDataWarning('No variables with local column definitions found in the semantic map.');
                return;
            }

            // Build the HTML for all database sections
            const databaseSectionsDiv = document.getElementById('databaseSections');
            let allHtml = '';
            let totalAnnotatedVars = 0;

            for (const [tableName, tableData] of Object.entries(tableVariables)) {
                const { variableInfo, graphdbName } = tableData;

                // Filter variables that are ready for annotation
                const annotatedVars = {};
                const unannotatedVars = [];

                for (const [varName, varData] of Object.entries(variableInfo)) {
                    if (!varData.predicate) {
                        unannotatedVars.push(`${varName} (missing predicate)`);
                        continue;
                    }
                    if (!varData.class) {
                        unannotatedVars.push(`${varName} (missing class)`);
                        continue;
                    }
                    if (!varData.local_definition) {
                        unannotatedVars.push(`${varName} (no local definition)`);
                        continue;
                    }
                    annotatedVars[varName] = varData;
                }

                console.log(`Table ${tableName}: ${Object.keys(annotatedVars).length} annotated, ${unannotatedVars.length} unannotated`);

                if (Object.keys(annotatedVars).length > 0) {
                    allHtml += this.renderDatabaseSection(graphdbName, annotatedVars);
                    totalAnnotatedVars += Object.keys(annotatedVars).length;
                }
            }

            console.log('Total annotated vars:', totalAnnotatedVars);

            if (totalAnnotatedVars === 0) {
                this.showNoDataWarning('No variables are ready for annotation. Please ensure variables have local definitions, predicates, and classes.');
                return;
            }

            databaseSectionsDiv.innerHTML = allHtml;

            // Hide loading, show content
            document.getElementById('loadingState').style.display = 'none';
            document.getElementById('annotationContent').style.display = 'block';

            // Initialize toggle buttons
            this.initializeToggleButtons();
        } catch (error) {
            console.error('Error in renderAnnotationData:', error);
            this.showNoDataWarning('An error occurred while processing the semantic map: ' + error.message);
        }
    },

    /**
     * Transform JSON-LD structure to variable_info organized by table
     * Only includes tables that have a match in GraphDB
     */
    transformJsonLdToVariableInfoByTable: function(data, matchingTables) {
        const result = {};
        const schemaVariables = data.schema?.variables || {};
        const databases = data.databases || {};

        // Create a map of jsonld table name -> graphdb name
        const tableToGraphDb = {};
        for (const match of matchingTables) {
            tableToGraphDb[match.jsonld] = match.graphdb;
        }

        // Process each database and table
        for (const [dbKey, dbData] of Object.entries(databases)) {
            if (!dbData.tables) continue;

            for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                // Determine the table name (sourceFile or tableKey)
                const tableName = (tableData.sourceFile) ? tableData.sourceFile : tableKey;

                // Skip if this table doesn't have a GraphDB match
                if (!tableToGraphDb[tableName]) continue;

                const graphdbName = tableToGraphDb[tableName];
                const variableInfo = {};

                if (!tableData.columns) continue;

                // Process columns for this table
                for (const [colKey, colData] of Object.entries(tableData.columns)) {
                    // Skip columns without localColumn defined
                    if (!colData.localColumn) continue;

                    // Extract variable name from mapsTo
                    if (!colData.mapsTo) continue;
                    const varName = colData.mapsTo.split('/').pop();
                    if (!varName) continue;

                    // Get the schema variable info
                    const varSchema = schemaVariables[varName] || {};

                    // Get local column name(s) - handle arrays
                    const localCols = Array.isArray(colData.localColumn) ? colData.localColumn : [colData.localColumn];
                    const filteredCols = localCols.filter(c => c);

                    // Skip if no valid local columns after filtering
                    if (filteredCols.length === 0) continue;

                    const localDef = filteredCols.join(', ');

                    // Get local mappings
                    const localMappings = colData.localMappings || {};

                    variableInfo[varName] = {
                        predicate: varSchema.predicate || null,
                        class: varSchema.class || null,
                        data_type: varSchema.dataType || null,
                        local_definition: localDef,
                        value_mapping: null
                    };

                    // Add value mapping if present in schema - only include terms with local mappings
                    if (varSchema.valueMapping && varSchema.valueMapping.terms) {
                        const terms = {};
                        let hasAnyLocalTerms = false;

                        for (const [termKey, termData] of Object.entries(varSchema.valueMapping.terms)) {
                            const localTermValue = localMappings[termKey];

                            // Skip if no local mapping defined for this term
                            if (localTermValue === undefined || localTermValue === null) continue;

                            // Handle array values - concatenate them
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

                        // Only add value_mapping if we have at least one term with local mapping
                        if (hasAnyLocalTerms) {
                            variableInfo[varName].value_mapping = { terms };
                        }
                    }
                }

                if (Object.keys(variableInfo).length > 0) {
                    result[tableName] = {
                        variableInfo: variableInfo,
                        graphdbName: graphdbName
                    };
                }
            }
        }

        return result;
    },

    /**
     * Render a database section with its variables
     */
    renderDatabaseSection: function(databaseName, variables) {
        const varCount = Object.keys(variables).length;
        const totalPages = Math.ceil(varCount / this.PAGE_SIZE);

        this.databaseVariables[databaseName] = Object.entries(variables);
        this.databasePages[databaseName] = 1;

        let html = `
            <h2 style="display: inline-block;"><i class="fas fa-database"></i> ${this.escapeHtml(databaseName)}</h2>
            <button class="toggle-button" type="button" data-database="${this.escapeHtml(databaseName)}">
                <span class="toggle-text">Show more</span>
                <svg class="angle-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" width="20" height="20">
                    <path d="M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z"/>
                </svg>
            </button>
            <div class="content hidden" data-database="${this.escapeHtml(databaseName)}">
                <div class="database-summary">
                    <strong>Database Summary:</strong> ${varCount} variable(s) ready for annotation
                </div>
                <div class="variables-container" id="variables-${this.escapeHtml(databaseName)}">
                </div>
                ${totalPages > 1 ? `
                <div class="pagination-controls" data-database="${this.escapeHtml(databaseName)}">
                    <button type="button" onclick="changePage('${this.escapeHtml(databaseName)}', -1)" disabled>&#x2190;</button>
                    <span class="page-indicator">Page <span class="current-page">1</span> of <span class="total-pages">${totalPages}</span></span>
                    <button type="button" onclick="changePage('${this.escapeHtml(databaseName)}', 1)">&#x2192;</button>
                </div>
                ` : ''}
            </div>
            <hr>
        `;

        return html;
    },

    /**
     * Render a specific page of variables for a database
     */
    renderPage: function(database, pageNumber) {
        const variables = this.databaseVariables[database];
        if (!variables) return;

        const totalPages = Math.ceil(variables.length / this.PAGE_SIZE);
        const startIdx = (pageNumber - 1) * this.PAGE_SIZE;
        const endIdx = Math.min(startIdx + this.PAGE_SIZE, variables.length);

        const container = document.getElementById(`variables-${database}`);
        if (!container) return;

        let html = '';
        for (let i = startIdx; i < endIdx; i++) {
            const [varName, varInfo] = variables[i];
            html += this.renderVariableCard(varName, varInfo);
        }
        container.innerHTML = html;

        const paginationControls = document.querySelector(`.pagination-controls[data-database="${database}"]`);
        if (paginationControls) {
            const prevBtn = paginationControls.querySelector('button:first-child');
            const nextBtn = paginationControls.querySelector('button:last-child');
            const currentPageSpan = paginationControls.querySelector('.current-page');

            if (prevBtn) prevBtn.disabled = pageNumber <= 1;
            if (nextBtn) nextBtn.disabled = pageNumber >= totalPages;
            if (currentPageSpan) currentPageSpan.textContent = pageNumber;
        }

        this.databasePages[database] = pageNumber;
    },

    /**
     * Change page for a specific database
     */
    changePage: function(database, direction) {
        const currentPage = this.databasePages[database] || 1;
        const variables = this.databaseVariables[database];
        if (!variables) return;

        const totalPages = Math.ceil(variables.length / this.PAGE_SIZE);
        const newPage = currentPage + direction;

        if (newPage >= 1 && newPage <= totalPages) {
            this.renderPage(database, newPage);
        }
    },

    /**
     * Render a variable card
     */
    renderVariableCard: function(varName, varInfo) {
        const hasLocalDef = !!varInfo.local_definition;
        const statusClass = hasLocalDef ? 'status-annotated' : 'status-unannotated';
        const statusIcon = hasLocalDef ? 'check-circle' : 'exclamation-triangle';
        const statusText = hasLocalDef ? 'Described' : 'Undescribed';

        let html = `
            <div class="variable-card">
                <div class="variable-header">
                    <div class="variable-name">${this.escapeHtml(varName)}</div>
                    <div class="status-badge ${statusClass}">
                        <i class="fas fa-${statusIcon}"></i> ${statusText}
                    </div>
                </div>
                <div class="variable-details">
                    <div class="detail-row">
                        <div class="detail-label">Local Definition:</div>
                        <div class="detail-value">${this.escapeHtml(varInfo.local_definition || 'Not specified')}</div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Predicate:</div>
                        <div class="detail-value"><code>${this.escapeHtml(varInfo.predicate || '')}</code></div>
                    </div>
                    <div class="detail-row">
                        <div class="detail-label">Class:</div>
                        <div class="detail-value"><code>${this.escapeHtml(varInfo.class || '')}</code></div>
                    </div>
        `;

        if (varInfo.data_type) {
            html += `
                    <div class="detail-row">
                        <div class="detail-label">Data Type:</div>
                        <div class="detail-value">${this.escapeHtml(varInfo.data_type)}</div>
                    </div>
            `;
        }

        // Render value mapping if present and has terms
        if (varInfo.value_mapping && varInfo.value_mapping.terms && Object.keys(varInfo.value_mapping.terms).length > 0) {
            html += `
                    <div class="detail-row">
                        <div class="detail-label">Value Mapping:</div>
                        <div class="detail-value">
                            <div class="value-mapping-section">
            `;

            for (const [term, termInfo] of Object.entries(varInfo.value_mapping.terms)) {
                const termLabel = term.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                const localValue = termInfo.local_term;
                const isEmptyValue = localValue === '';
                const displayValue = isEmptyValue ? '(empty)' : localValue;
                const valueClass = isEmptyValue ? 'local-value empty-value' : 'local-value';

                html += `
                            <div class="value-mapping-item">
                                <span class="term-name">${this.escapeHtml(termLabel)}</span>
                                <span class="${valueClass}">${this.escapeHtml(displayValue)}</span>
                                <span class="target-class"><code>${this.escapeHtml(termInfo.target_class || '')}</code></span>
                            </div>
                `;
            }

            html += `
                            </div>
                        </div>
                    </div>
            `;
        }

        html += `
                </div>
            </div>
        `;

        return html;
    },

    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml: function(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    },

    /**
     * Initialize toggle button functionality
     */
    initializeToggleButtons: function() {
        var self = this;
        $('.toggle-button').off('click').on('click', function () {
            var textElement = $(this).find('.toggle-text');
            var svgElement = $(this).find('.angle-icon path');
            var database = $(this).data('database');
            var isOpening = textElement.text() === 'Show more';

            if (isOpening) {
                textElement.text('Show less');
                svgElement.attr('d', 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160-160z');
                if (database && self.databaseVariables[database]) {
                    self.renderPage(database, self.databasePages[database] || 1);
                }
            } else {
                textElement.text('Show more');
                svgElement.attr('d', 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z');
            }
            $(this).toggleClass('open');
            $(this).next('.content').toggleClass('active');
            $(this).next('.content').removeClass('hidden');
        });
    },

    /**
     * Start the annotation process - submit semantic map to backend first
     */
    startAnnotationProcess: async function() {
        const button = document.getElementById('startAnnotation');
        const originalHtml = button.innerHTML;

        if (!this.semanticMapData) {
            alert('No semantic map data available. Please reload the page.');
            return;
        }

        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Submitting semantic map...';

        try {
            // First, submit the semantic map to the backend
            const submitResponse = await fetch('/submit-indexeddb-semantic-map', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
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
                button.disabled = false;
                button.innerHTML = originalHtml;
                return;
            }

            // Now start the annotation process
            button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing Annotations...';

            const annotationResponse = await fetch('/start-annotation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            });

            const annotationData = await annotationResponse.json();

            if (annotationData.success) {
                window.location.href = '/annotation-verify';
            } else {
                alert('Error: ' + annotationData.error);
                button.disabled = false;
                button.innerHTML = originalHtml;
            }
        } catch (error) {
            console.error('Error:', error);
            alert('An error occurred while processing. Please try again.');
            button.disabled = false;
            button.innerHTML = originalHtml;
        }
    }
};

// Global functions for onclick handlers in HTML
function changePage(database, direction) {
    AnnotationReviewPage.changePage(database, direction);
}

function startAnnotationProcess() {
    AnnotationReviewPage.startAnnotationProcess();
}

// Export for global access
window.AnnotationReviewPage = AnnotationReviewPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AnnotationReviewPage;
}

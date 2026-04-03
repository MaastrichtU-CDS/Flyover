/**
 * Annotation Landing App - Vue.js 3 Component
 *
 * Replaces the AnnotationLandingPage module with a reactive Vue 3 application
 * using the Options API. Handles JSON-LD upload, IndexedDB semantic map
 * detection, and RDF store database comparison.
 *
 * Uses [[ ]] delimiters to avoid conflicts with Jinja2 {{ }} syntax.
 * Mounts to the #annotation-landing-app element in the DOM.
 *
 * @requires Vue 3 CDN (loaded in HTML template)
 * @requires FlyoverDB (from db-utils.js)
 */

const AnnotationLandingApp = Vue.createApp({
    delimiters: ['[[', ']]'],

    /**
     * Component template containing:
     * - Option 1: JSON-LD Upload Section
     * - Option 2: Describe Existing Data
     * - Option 3: Continue with IndexedDB Semantic Map (hidden by default)
     */
    template: `
    <div>
        <!-- Option 1: Upload JSON-LD -->
        <div id="jsonUploadSection" class="mb-4">
            <div class="card">
                <div class="card-header">
                    <h5 class="mb-0">
                        <i class="fas fa-project-diagram"></i> Option 1: Upload Finalised Flyover Semantic Map (JSON-LD) for Direct Annotation
                    </h5>
                </div>
                <div class="card-body">
                    <p class="text-muted">
                        If you have a finalised semantic map JSON-LD file with data descriptions and semantic mappings, you can
                        upload it directly for annotation processing.
                    </p>

                    <!-- Warning when existing JSON-LD is in IndexedDB -->
                    <div v-if="existingJsonldWarning" class="alert alert-warning" style="font-size: 0.9em;">
                        <i class="fas fa-exclamation-triangle"></i>
                        <strong>Existing semantic map detected!</strong><br>
                        You already have a semantic map stored in your browser from a previous session.
                        Uploading a new JSON-LD file will <strong>overwrite</strong> the existing semantic map.
                        <br><small class="text-muted">Current semantic map data source(s): <span>[[ existingJsonldDatabases ]]</span></small>
                    </div>

                    <!-- Upload status messages -->
                    <div v-if="uploadStatus.message" v-html="uploadStatusHtml" style="font-size: 0.9em;"></div>

                    <div v-if="showUploadInfo">
                        <div class="alert alert-info"
                             style="font-size: 0.85em; border-left: 4px solid rgba(118,75,162,0.75); background: linear-gradient(135deg, rgba(102,126,234,0.75) 0%, rgba(118,75,162,0.75) 100%); color: white">
                            <i class="fas fa-info-circle"></i>
                            <strong>What should the JSON-LD contain?</strong><br>
                            Your JSON-LD file should include data variable definitions, types, and any pre-existing semantic
                            mappings that you want to enhance with additional annotations.
                        </div>

                        <form @submit.prevent="handleJsonUpload" id="jsonUploadForm">
                            <div class="form-group">
                                <label for="annotationJsonPath">JSON-LD File for Annotation:</label>
                                <div class="input-group">
                                    <input type="text" id="annotationJsonPath" name="annotationJsonPath"
                                           class="form-control"
                                           :value="selectedFileName"
                                           placeholder="Select JSON-LD file..." readonly>
                                    <div class="input-group-append">
                                        <button type="button" class="btn btn-outline-secondary"
                                                @click="$refs.fileInput.click()">
                                            <i class="fas fa-folder-open"></i> Browse
                                        </button>
                                    </div>
                                </div>
                                <input type="file" ref="fileInput" style="display:none;"
                                       accept=".jsonld" @change="updateAnnotationJsonPath">
                                <small class="form-text text-muted">
                                    Upload a JSON-LD file containing your data descriptions and variable mappings.
                                </small>
                            </div>
                        </form>
                    </div>

                    <!-- Button stays visible after successful upload -->
                    <div class="form-group mb-0">
                        <button v-if="!continueToReview"
                                type="submit" form="jsonUploadForm"
                                class="btn"
                                :class="uploadButtonState.btnClass"
                                :disabled="uploadButtonState.disabled"
                                v-html="uploadButtonState.text">
                        </button>
                        <button v-else
                                type="button"
                                class="btn btn-primary"
                                @click="goToAnnotationReview">
                            <i class="fas fa-arrow-right"></i> Proceed to Annotation Review
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Option 2: Describe Existing Data -->
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">
                    <i class="fas fa-edit"></i> Option 2: Describe Existing Data
                </h5>
            </div>
            <div class="card-body">
                <p class="text-muted">
                    Use the data that has already been uploaded and describe it in the "Describe" steps.
                </p>
                <form method="GET" action="/describe_landing">
                    <button type="submit" class="btn btn-primary">
                        <i class="fas fa-fast-backward"></i> Start Describing your data
                    </button>
                </form>
            </div>
        </div>

        <!-- Option 3: Continue with IndexedDB Semantic Map -->
        <div v-if="indexedDbSection.show" class="card mb-4">
            <div class="card-header">
                <h5 class="mb-0">
                    <i class="fas fa-history"></i> Option 3: Continue with Existing Semantic Map
                </h5>
            </div>
            <div class="card-body">
                <p class="text-muted">
                    A semantic map from your previous session was found in your browser storage.
                </p>

                <div v-if="indexedDbMatchInfo" v-html="indexedDbMatchInfo" class="mb-3"></div>

                <a v-if="indexedDbSection.continueToReview" href="/annotation-review"
                   class="btn btn-primary">
                    <i class="fas fa-arrow-right"></i> Continue to Annotation Review
                </a>

                <div v-if="indexedDbSection.errorHtml" v-html="indexedDbSection.errorHtml"></div>
            </div>
        </div>
    </div>
    `,

    /**
     * Reactive data for the annotation landing page.
     * @returns {Object} Component state
     */
    data() {
        return {
            /** @type {string[]} Databases available in the RDF store */
            rdfStoreDatabases: [],
            /** @type {boolean} Whether an existing JSON-LD semantic map was found in IndexedDB */
            existingJsonldWarning: false,
            /** @type {string} Comma-separated list of databases from the existing semantic map */
            existingJsonldDatabases: '-',
            /** @type {{ message: string, type: string }} Upload status message and type */
            uploadStatus: { message: '', type: '' },
            /** @type {boolean} Whether to show the upload info section (file input + instructions) */
            showUploadInfo: true,
            /** @type {boolean} Whether the upload is currently processing */
            uploadProcessing: false,
            /** @type {string} Display name of the selected file */
            selectedFileName: '',
            /** @type {File|null} The selected JSON-LD file object */
            jsonFile: null,
            /** @type {{ show: boolean, continueToReview: boolean, errorHtml: string }} IndexedDB section state */
            indexedDbSection: {
                show: false,
                continueToReview: false,
                errorHtml: ''
            },
            /** @type {string} HTML content showing database match comparison */
            indexedDbMatchInfo: '',
            /** @type {boolean} Whether upload succeeded and button should link to review */
            continueToReview: false
        };
    },

    computed: {
        /**
         * Computed button state based on upload processing and file selection.
         * @returns {{ text: string, disabled: boolean, btnClass: string }}
         */
        uploadButtonState() {
            if (this.uploadProcessing) {
                return {
                    text: '<i class="fas fa-spinner fa-spin"></i> Uploading...',
                    disabled: true,
                    btnClass: 'btn-success'
                };
            }
            return {
                text: '<i class="fas fa-upload"></i> Upload JSON-LD for Annotation',
                disabled: !this.selectedFileName,
                btnClass: 'btn-success'
            };
        },

        /**
         * Computed HTML for the upload status alert.
         * @returns {string} Alert HTML string
         */
        uploadStatusHtml() {
            if (!this.uploadStatus.message) return '';
            const type = this.uploadStatus.type || 'success';
            const alertClass = type === 'error' ? 'alert-danger' :
                type === 'info' ? 'alert-info' : 'alert-success';
            const iconClass = type === 'error' ? 'exclamation-triangle' :
                type === 'info' ? 'info-circle' : 'check-circle';
            return `<div class="alert ${alertClass}" style="font-size: 0.9em;">` +
                `<i class="fas fa-${iconClass}"></i> ${this.uploadStatus.message}</div>`;
        }
    },

    methods: {
        /**
         * Check IndexedDB for an existing semantic map and compare with RDF store databases.
         * Shows Option 3 section if a semantic map is found.
         */
        async checkIndexedDbSemanticMap() {
            try {
                if (typeof FlyoverDB === 'undefined') {
                    console.warn('FlyoverDB not available');
                    return;
                }

                await FlyoverDB.initDB();
                this.rdfStoreDatabases = await this.fetchAndStoreRdfStoreDatabases();

                const result = await FlyoverDB.getData('metadata', 'semantic_map');

                if (result && result.data) {
                    console.log('IndexedDB: Found semantic map', result);

                    this.indexedDbSection.show = true;

                    const jsonldTables = this.extractJsonLdTables(result.data);
                    const comparison = this.generateDatabaseComparisonHtml(jsonldTables, this.rdfStoreDatabases);
                    this.indexedDbMatchInfo = comparison.html;

                    if (comparison.hasMatches) {
                        this.indexedDbSection.continueToReview = true;
                        this.indexedDbSection.errorHtml = '';
                    } else {
                        this.indexedDbSection.continueToReview = false;
                        this.indexedDbSection.errorHtml = `
                            <div class="alert alert-danger mt-3" style="font-size: 0.9em;">
                                <i class="fas fa-times-circle"></i>
                                <strong>Cannot proceed:</strong> No matching data sources found between your semantic map and the RDF store.
                            </div>`;
                    }

                    this.existingJsonldWarning = true;
                    this.existingJsonldDatabases = jsonldTables.join(', ') || 'Unknown';
                } else {
                    console.log('IndexedDB: No semantic map found');
                    this.existingJsonldWarning = false;
                }
            } catch (error) {
                console.error('Error checking IndexedDB for semantic map:', error);
            }
        },

        /**
         * Fetch databases from the RDF store API and persist to IndexedDB.
         * @returns {Promise<string[]>} List of database names
         */
        async fetchAndStoreRdfStoreDatabases() {
            try {
                const response = await fetch('/api/rdf-store-databases');
                const data = await response.json();

                if (data.success && data.databases && data.databases.length > 0) {
                    console.log('Fetched RDF store databases:', data.databases);
                    this.rdfStoreDatabases = data.databases;

                    await FlyoverDB.saveData('metadata', {
                        key: 'rdf_store_databases',
                        data: data.databases,
                        timestamp: new Date().toISOString()
                    });

                    return data.databases;
                } else {
                    console.warn('No databases found in RDF store:', data.message);
                    return [];
                }
            } catch (error) {
                console.error('Error fetching RDF store databases:', error);
                return [];
            }
        },

        /**
         * Handle JSON-LD file upload: read, validate, POST to server, store in IndexedDB.
         * On success, hides upload section and shows "Proceed to Annotation Review" button.
         */
        async handleJsonUpload() {
            if (!this.jsonFile) {
                this.showUploadStatus('Please select a JSON-LD file to upload.', 'error');
                return;
            }

            this.uploadProcessing = true;
            this.uploadStatus = { message: '', type: '' };

            try {
                const fileContent = await this.readFileAsText(this.jsonFile);
                let jsonData;

                try {
                    jsonData = JSON.parse(fileContent);
                } catch (parseError) {
                    this.showUploadStatus('Invalid JSON-LD file format. Please check your file.', 'error');
                    this.uploadProcessing = false;
                    return;
                }

                const formData = new FormData();
                formData.append('annotationJsonFile', this.jsonFile);

                const response = await fetch('/upload-annotation-json', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (response.ok && result.success) {
                    try {
                        await FlyoverDB.saveData('metadata', {
                            key: 'semantic_map',
                            data: jsonData,
                            timestamp: new Date().toISOString()
                        });
                        console.log('Saved uploaded JSON-LD to IndexedDB');
                    } catch (dbError) {
                        console.error('Failed to save JSON-LD to IndexedDB:', dbError);
                    }

                    let message = '<strong>JSON-LD file uploaded successfully!</strong><br>';

                    if (result.matching_databases && result.matching_databases.length > 0) {
                        message += `<i class="fas fa-check-circle text-success"></i> ${result.matching_databases.length} data source(s) matched and ready for annotation.<br>`;
                    }

                    if (result.non_matching_jsonld && result.non_matching_jsonld.length > 0) {
                        message += `<i class="fas fa-exclamation-triangle text-warning"></i> ${result.non_matching_jsonld.length} data source(s) in semantic map not found in RDF store.<br>`;
                    }

                    this.showUploadStatus(message, 'success');
                    this.showUploadInfo = false;
                    this.existingJsonldWarning = false;
                    this.indexedDbSection.show = false;
                    this.continueToReview = true;
                } else {
                    let errorMessage = result.error || 'Failed to upload JSON-LD file.';

                    if (result.rdf_store_databases && result.rdf_store_databases.length > 0) {
                        errorMessage += '<br><br><strong>Data available in RDF store:</strong><ul>';
                        result.rdf_store_databases.forEach(db => {
                            errorMessage += `<li>${this.escapeHtml(db)}</li>`;
                        });
                        errorMessage += '</ul>';
                    }

                    if (result.jsonld_databases && result.jsonld_databases.length > 0) {
                        errorMessage += '<strong>Data sources in uploaded semantic map:</strong><ul>';
                        result.jsonld_databases.forEach(db => {
                            errorMessage += `<li>${this.escapeHtml(db)}</li>`;
                        });
                        errorMessage += '</ul>';
                    }

                    this.showUploadStatus(errorMessage, 'error');
                }
            } catch (error) {
                console.error('Upload error:', error);
                this.showUploadStatus('Failed to upload file: ' + error.message, 'error');
            } finally {
                this.uploadProcessing = false;
            }
        },

        /**
         * Update file path display when user selects a file.
         * @param {Event} event - The file input change event
         */
        updateAnnotationJsonPath(event) {
            const input = event.target;
            const fullPath = input.value;
            const fileName = fullPath.split('\\').pop();
            this.selectedFileName = fileName;
            this.jsonFile = input.files.length ? input.files[0] : null;
        },

        /**
         * Show an upload status message inside the Option 1 card.
         * @param {string} message - Message to display
         * @param {string} type - Message type: 'success', 'error', 'info'
         */
        showUploadStatus(message, type) {
            this.uploadStatus = { message: message, type: type || 'success' };
        },

        /**
         * Escape HTML to prevent XSS.
         * Delegates to StatusMessages.escapeHtml if available.
         * @param {string} text - Text to escape
         * @returns {string} Escaped text
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
         * Extract table names from a JSON-LD data structure.
         * @param {Object} data - JSON-LD data object
         * @returns {string[]} Array of table/source file names
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
         * Generate HTML comparing JSON-LD tables against RDF store databases.
         * @param {string[]} jsonldTables - Table names from the JSON-LD
         * @param {string[]} rdfStoreList - Database names in the RDF store
         * @returns {{ html: string, hasMatches: boolean, matching: Object[], nonMatchingJsonld: string[], nonMatchingRdfStore: string[] }}
         */
        generateDatabaseComparisonHtml(jsonldTables, rdfStoreList) {
            const matching = [];
            const nonMatchingJsonld = [];
            const nonMatchingRdfStore = [...rdfStoreList];

            for (const jsonldTable of jsonldTables) {
                const match = this.findMatchingDatabase(jsonldTable, rdfStoreList);
                if (match) {
                    matching.push({ jsonld: jsonldTable, rdf_store: match });
                    const idx = nonMatchingRdfStore.indexOf(match);
                    if (idx > -1) nonMatchingRdfStore.splice(idx, 1);
                } else {
                    nonMatchingJsonld.push(jsonldTable);
                }
            }

            let html = '';

            if (matching.length > 0) {
                html += `<div class="text-success mb-2" style="font-size: 0.9em;">
                    <i class="fas fa-check-circle"></i> <strong>${matching.length}</strong> data source(s) ready for annotation
                </div>`;
            }

            if (nonMatchingJsonld.length > 0) {
                html += `<div class="text-warning mb-2" style="font-size: 0.9em;">
                    <i class="fas fa-exclamation-triangle"></i> <strong>Not in RDF store:</strong> ${nonMatchingJsonld.map(t => this.escapeHtml(t)).join(', ')}
                </div>`;
            }

            if (nonMatchingRdfStore.length > 0) {
                html += `<div class="text-muted mb-2" style="font-size: 0.9em;">
                    <i class="fas fa-info-circle"></i> <strong>Other data in RDF store:</strong> ${nonMatchingRdfStore.map(t => this.escapeHtml(t)).join(', ')}
                </div>`;
            }

            return {
                html: html,
                hasMatches: matching.length > 0,
                matching: matching,
                nonMatchingJsonld: nonMatchingJsonld,
                nonMatchingRdfStore: nonMatchingRdfStore
            };
        },

        /**
         * Check if a database name matches any RDF store database (exact or sans .csv).
         * @param {string} mapDbName - Database name from the semantic map
         * @param {string[]} rdfStoreList - List of RDF store database names
         * @returns {string|null} Matching database name or null
         */
        findMatchingDatabase(mapDbName, rdfStoreList) {
            if (!mapDbName || mapDbName === '') return null;

            for (const db of rdfStoreList) {
                if (db === mapDbName) return db;

                const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
                const dbNoExt = db.endsWith('.csv') ? db.slice(0, -4) : db;
                if (mapNoExt === dbNoExt) return db;
            }
            return null;
        },

        /**
         * Read a File object as text using FileReader.
         * @param {File} file - The file to read
         * @returns {Promise<string>} File contents as text
         */
        readFileAsText(file) {
            return new Promise((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = (e) => resolve(e.target.result);
                reader.onerror = () => reject(new Error('Failed to read file'));
                reader.readAsText(file);
            });
        },

        /**
         * Navigate to the annotation review page.
         */
        goToAnnotationReview() {
            window.location.href = '/annotation-review';
        }
    },

    /**
     * Lifecycle hook: check IndexedDB for existing semantic map on mount.
     */
    mounted() {
        this.checkIndexedDbSemanticMap();
    }
});

/**
 * Global bridge function for backward compatibility.
 * File input changes are now handled by Vue's @change binding.
 * @param {HTMLInputElement} input - The file input element (unused)
 */
function updateAnnotationJsonPath(input) {
    // Now handled by Vue
}

// Export for global access
window.AnnotationLandingApp = AnnotationLandingApp;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AnnotationLandingApp;
}

/**
 * Ingest App - Vue.js 3 Component
 *
 * Replaces the IngestPage module with a reactive Vue 3 application using the Options API.
 * Handles data ingestion page functionality including CSV upload, PostgreSQL configuration,
 * PK/FK configuration, and cross-graph data linking.
 *
 * Uses [[ ]] delimiters to avoid conflicts with Jinja2 {{ }} syntax.
 * Mounts to the #ingest-app element in the DOM.
 *
 * @requires Vue 3 CDN (loaded in HTML template)
 * @requires jQuery (for Bootstrap interactions and animations)
 */

const IngestApp = Vue.createApp({
    delimiters: ['[[', ']]'],

    /**
     * Component template containing all form HTML with Vue directives.
     * Mirrors the original ingest.html form (lines 45-208) but uses Vue reactive bindings.
     */
    template: `
    <form method="POST" action="/upload" enctype="multipart/form-data" id="mainForm"
          @submit="onFormSubmit">

        <!-- Data Source Selection -->
        <div class="form-group">
            <label for="CSV"><i class="fas fa-file-csv"></i> CSV:</label>
            <input type="radio" id="CSV" name="fileType" value="CSV"
                   v-model="fileType">
        </div>

        <div class="form-group">
            <label for="Postgres"><i class="fas fa-database"></i> PostgreSQL:</label>
            <input type="radio" id="Postgres" name="fileType" value="Postgres"
                   v-model="fileType">
        </div>

        <!-- CSV Form Section -->
        <div id="csvform" v-show="fileType === 'CSV'">
            <hr>
            <label for="csvPath">Please specify the path of the CSV file(s) you would like to process</label>
            <br>
            <input type="text" id="csvPath" name="csvPath" :value="csvPath"
                   placeholder="Enter CSV File Path(s)" readonly>
            <input type="button" class="btn btn-primary" value="..."
                   @click="triggerFileInput">
            <input type="file" id="csvFile" name="csvFile" ref="csvFileInput"
                   style="display:none;" multiple accept=".csv"
                   @change="handleFileChange">
            <input type="text" id="csv_separator_sign" name="csv_separator_sign"
                   v-model="csvSeparatorSign"
                   placeholder="Separator sign (defaults to ',')"
                   style="width: 250px; margin-left: 10px;">
            <input type="text" id="csv_decimal_sign" name="csv_decimal_sign"
                   v-model="csvDecimalSign"
                   placeholder="Decimal sign (defaults to '.')"
                   style="width: 250px; margin-left: 10px;">

            <!-- PK/FK Configuration Section -->
            <div id="pkFkSection" v-show="showPkFkSection" style="margin-top: 20px;">
                <hr>
                <div class="alert alert-info">
                    <strong><i class="fas fa-info-circle"></i>Multiple CSV Files Detected</strong><br>
                    To establish relationships between your data files, you can optionally specify Primary Keys (PK) and Foreign Keys (FK) for each file. <br> This will help create meaningful connections in your knowledge graph.
                </div>
                <div id="pkFkContent">
                    <!-- Dynamic PK/FK fields per file -->
                    <div class="card mb-3" v-for="(file, index) in csvFiles" :key="file.name">
                        <div class="card-header bg-light">
                            <h6 class="mb-0">
                                <i class="fas fa-table"></i> [[ file.name ]]
                                <small class="text-muted">([[ getFileColumns(file.name).length ]] columns detected)</small>
                            </h6>
                        </div>
                        <div class="card-body">
                            <div class="row">
                                <div class="col-md-6">
                                    <div class="form-group">
                                        <label :for="'pk_' + index" class="font-weight-bold">
                                            <i class="fas fa-key text-warning"></i> Primary Key:
                                        </label>
                                        <select class="form-control" :id="'pk_' + index" :name="'pk_' + index"
                                                v-model="pkSelections[index]">
                                            <option value="">-- No Primary Key --</option>
                                            <option v-for="col in getFileColumns(file.name)" :key="col" :value="col">
                                                [[ col ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">
                                            Column that uniquely identifies each row in this file
                                        </small>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <div class="form-group">
                                        <label :for="'fk_' + index" class="font-weight-bold">
                                            <i class="fas fa-link text-info"></i> Foreign Key:
                                        </label>
                                        <select class="form-control" :id="'fk_' + index" :name="'fk_' + index"
                                                v-model="fkSelections[index]">
                                            <option value="">-- No Foreign Key --</option>
                                            <option v-for="col in getFileColumns(file.name)" :key="col" :value="col">
                                                [[ col ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">
                                            Column that references another table's primary key
                                        </small>
                                    </div>
                                </div>
                            </div>
                            <div class="row" :id="'fkDetails_' + index" v-show="fkSelections[index]">
                                <div class="col-md-6">
                                    <div class="form-group">
                                        <label :for="'fkTable_' + index" class="font-weight-bold">
                                            <i class="fas fa-arrow-right text-success"></i> References Table:
                                        </label>
                                        <select class="form-control" :id="'fkTable_' + index" :name="'fkTable_' + index"
                                                v-model="fkTableSelections[index]"
                                                @change="onFkTableChange(index)">
                                            <option value="">-- Select Referenced Table --</option>
                                            <option v-for="otherFile in getOtherFiles(file.name)"
                                                    :key="otherFile.name" :value="otherFile.name">
                                                [[ otherFile.name ]]
                                            </option>
                                        </select>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <div class="form-group">
                                        <label :for="'fkColumn_' + index" class="font-weight-bold">
                                            <i class="fas fa-arrow-right text-success"></i> References Column:
                                        </label>
                                        <select class="form-control" :id="'fkColumn_' + index" :name="'fkColumn_' + index"
                                                v-model="fkColumnSelections[index]">
                                            <option value="">-- Select Referenced Column --</option>
                                            <option v-for="col in getFileColumns(fkTableSelections[index])"
                                                    :key="col" :value="col">
                                                [[ col ]]
                                            </option>
                                        </select>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Tips -->
                    <div class="alert alert-light border">
                        <h6><i class="fas fa-lightbulb text-warning"></i> Tips:</h6>
                        <ul class="mb-0 small">
                            <li><strong>Primary Key:</strong> A column that uniquely identifies each row (e.g., ID, PatientID)</li>
                            <li><strong>Foreign Key:</strong> A column that references a primary key in another table</li>
                            <li>Leave fields empty if no relationships exist</li>
                            <li>These relationships will be used to create connections in your knowledge graph</li>
                        </ul>
                    </div>
                </div>
            </div>

            <!-- Data Linking Section - only shown when graphExists and files uploaded -->
            <div id="dataLinkingSection" v-if="graphExists" v-show="showDataLinkingSection"
                 style="margin-top: 20px;">
                <hr>
                <div class="alert alert-info">
                    <strong><i class="fas fa-link"></i> Link Your New Data to Existing Graph Data</strong><br>
                    <p>This feature allows you to connect your new data with data that already exists in the graph database. </p>
                    <p class="mb-0"><small>Note: Make sure your linking columns contain compatible values (e.g., identical ID formats)</small></p>
                </div>

                <div class="form-check mb-3">
                    <input class="form-check-input" type="checkbox" id="enableDataLinking"
                           name="enableDataLinking" v-model="enableDataLinking">
                    <label class="form-check-label" for="enableDataLinking">
                        <strong>Enable Data Linking</strong>
                    </label>
                    <small class="form-text text-muted">Check this box to establish connections between your new data and existing data</small>
                </div>

                <div id="linkingConfiguration" v-show="enableDataLinking">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0">Configure Data Links</h6>
                        </div>
                        <div class="card-body">
                            <div class="row">
                                <div class="col-md-6">
                                    <h6>New Data (Data you're uploading now)</h6>
                                    <div class="form-group">
                                        <label for="newTableName">Select Data File:</label>
                                        <select class="form-control" id="newTableName" name="newTableName"
                                                v-model="newTableName">
                                            <option value="">-- Select the CSV file you want to link --</option>
                                            <option v-for="file in csvFiles" :key="file.name"
                                                    :value="tableName(file.name)">
                                                [[ tableName(file.name) ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">Choose which of your uploaded CSV files contains the linking information</small>
                                    </div>
                                    <div class="form-group">
                                        <label for="newColumnName">Select Linking Column:</label>
                                        <select class="form-control" id="newColumnName" name="newColumnName"
                                                v-model="newColumnName">
                                            <option value="">-- Select the column containing identifiers --</option>
                                            <option v-for="col in newTableColumns" :key="col" :value="col">
                                                [[ col ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">Choose the column that contains values matching the existing data (e.g., Patient ID, Study Number)</small>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <h6>Existing Graph Data (Data already in the graph database)</h6>
                                    <div class="form-group">
                                        <label for="existingTableName">Select Existing Table:</label>
                                        <select class="form-control" id="existingTableName" name="existingTableName"
                                                v-model="existingTableName">
                                            <option value="">-- Select the existing data table to link to --</option>
                                            <option v-for="table in existingTables" :key="table" :value="table">
                                                [[ table ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">Choose which existing data table you want to link with</small>
                                    </div>
                                    <div class="form-group">
                                        <label for="existingColumnName">Select Matching Column:</label>
                                        <select class="form-control" id="existingColumnName" name="existingColumnName"
                                                v-model="existingColumnName">
                                            <option value="">-- Select the column to match against --</option>
                                            <option v-for="col in existingTableColumns" :key="col" :value="col">
                                                [[ col ]]
                                            </option>
                                        </select>
                                        <small class="form-text text-muted">Choose the column from existing data that matches your new data's identifiers</small>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <br>
                </div>

                <!-- Hidden field for cross-graph linking data -->
                <input type="hidden" id="crossGraphLinkData" name="crossGraphLinkData"
                       :value="crossGraphLinkDataJson">
            </div>
        </div>

        <!-- PostgreSQL Form Section -->
        <div id="postgresform" v-show="fileType === 'Postgres'">
            <hr>
            <label for="username">Please specify the following details for your postgres database</label>
            <br>
            <label for="username">PostgreSQL Username:</label>
            <input type="text" id="username" name="username" v-model="pgUsername">
            <br>
            <label for="password">PostgreSQL Password:</label>
            <input type="password" id="password" name="password" v-model="pgPassword">
            <br>
            <label for="POSTGRES_URL">PostgreSQL URL:</label>
            <input type="text" id="POSTGRES_URL" name="POSTGRES_URL" v-model="pgUrl">
            <br>
            <label for="POSTGRES_DB">PostgreSQL Database:</label>
            <input type="text" id="POSTGRES_DB" name="POSTGRES_DB" v-model="pgDb">
        </div>

        <!-- Hidden field for PK/FK data -->
        <input type="hidden" id="pkFkData" name="pkFkData" :value="pkFkDataJson">

        <button type="submit" id="submitFilesButton" class="btn btn-primary"
                :disabled="!isFormValid"
                :title="submitButtonTitle">
            <i class="fas fa-play"></i> Submit Files
        </button>

        <div class="mt-4">
            <div class="alert alert-info py-2" style="font-size: 0.85em; border-left: 4px solid rgba(118,75,162,0.75); background: linear-gradient(135deg, rgba(102,126,234,0.75) 0%, rgba(118,75,162,0.75) 100%); color: white">
                <i class="fas fa-info-circle"></i>
                <strong>Directly uploading your graph data</strong><br>
                <div class="mt-1 ms-4">
                    <div class="mt-1">
                        <p class="mb-1">
                            Please know that if you have already converted your data using Flyover, then you can also directly upload the files in the RDF store interface.
                            <br>
                            In that case you can skip this data ingest step and proceed to describing and or annotating your data.
                        </p>
                    </div>
                </div>
            </div>
        </div>

        <template v-if="graphExists">
            <hr>
            <p><i class="fas fa-exclamation-triangle"
                  style="margin-right: 5px; align-self: flex-start;"></i><i><b>The associated RDF store already contains a data graph.</b></i><br>
                <i>It is possible to add more data to this graph, for that you can use the data upload available
                    above.</i><br>
                <i>It is, however, possible to describe the existing data without having to add new,
                    for that please use the options below.</i></p>
            <i>The existing data graph can also be removed through the RDF store interface.</i></p>
            <br>
            <button type="submit" id="submitWithoutData" class="btn btn-primary"
                    @click.prevent="submitWithoutData">
                <i class="fas fa-fast-forward"></i> Proceed without adding data
            </button>
            <br>
            <br>
        </template>
    </form>
    `,

    /**
     * Reactive data properties.
     * @returns {Object} Component state
     */
    data() {
        return {
            /** @type {string} Selected data source type ('CSV' or 'Postgres') */
            fileType: '',

            /** @type {Array<File>} Currently selected CSV files */
            csvFiles: [],

            /** @type {Object<string, string[]>} Map of filename to column headers */
            csvColumns: {},

            /** @type {string} Display string of selected file paths */
            csvPath: '',

            /** @type {string} User-specified CSV separator sign */
            csvSeparatorSign: '',

            /** @type {string} User-specified CSV decimal sign */
            csvDecimalSign: '',

            /** @type {boolean} Whether a graph already exists in the RDF store */
            graphExists: window.graph_exists || false,

            /** @type {Object|null} Structure of the existing graph from the server */
            existingGraphStructure: null,

            /** @type {boolean} Whether data linking is enabled */
            enableDataLinking: false,

            /** @type {string} Selected new table name for data linking */
            newTableName: '',

            /** @type {string} Selected new column name for data linking */
            newColumnName: '',

            /** @type {string} Selected existing table name for data linking */
            existingTableName: '',

            /** @type {string} Selected existing column name for data linking */
            existingColumnName: '',

            /** @type {string} PostgreSQL username */
            pgUsername: '',

            /** @type {string} PostgreSQL password */
            pgPassword: '',

            /** @type {string} PostgreSQL URL */
            pgUrl: '',

            /** @type {string} PostgreSQL database name */
            pgDb: '',

            /** @type {Object<number, string>} PK selections keyed by file index */
            pkSelections: {},

            /** @type {Object<number, string>} FK selections keyed by file index */
            fkSelections: {},

            /** @type {Object<number, string>} FK referenced table selections keyed by file index */
            fkTableSelections: {},

            /** @type {Object<number, string>} FK referenced column selections keyed by file index */
            fkColumnSelections: {},

            /** @type {boolean} Whether the PK/FK section is visible */
            showPkFkSection: false,

            /** @type {boolean} Whether the data linking section is visible */
            showDataLinkingSection: false
        };
    },

    computed: {
        /**
         * Columns for the currently selected new table in data linking.
         * @returns {string[]} Column names
         */
        newTableColumns() {
            if (!this.newTableName) return [];
            const matchingFile = this.csvFiles.find(
                f => f.name.replace('.csv', '') === this.newTableName
            );
            if (matchingFile && this.csvColumns[matchingFile.name]) {
                return this.csvColumns[matchingFile.name];
            }
            return [];
        },

        /**
         * List of existing table names from the graph structure.
         * @returns {string[]} Table names
         */
        existingTables() {
            if (this.existingGraphStructure && this.existingGraphStructure.tables) {
                return this.existingGraphStructure.tables;
            }
            return [];
        },

        /**
         * Columns for the currently selected existing table in data linking.
         * @returns {string[]} Column names
         */
        existingTableColumns() {
            if (!this.existingTableName || !this.existingGraphStructure ||
                !this.existingGraphStructure.tableColumns) {
                return [];
            }
            return this.existingGraphStructure.tableColumns[this.existingTableName] || [];
        },

        /**
         * Whether the main form passes basic validation for submission.
         * @returns {boolean}
         */
        isFormValid() {
            const basicValidation =
                (this.fileType === 'CSV' && this.csvFiles.length > 0) ||
                (this.fileType === 'Postgres' && this.pgUsername && this.pgPassword &&
                 this.pgUrl && this.pgDb);

            const pkFkValid = this.validatePkFkRelationships();

            return basicValidation && pkFkValid;
        },

        /**
         * Tooltip text for the submit button when disabled.
         * @returns {string}
         */
        submitButtonTitle() {
            if (this.isFormValid) return '';
            if (!this.validatePkFkRelationships()) {
                return 'Please select primary keys for all tables that are referenced by foreign keys';
            }
            return '';
        },

        /**
         * Serialized PK/FK data for the hidden form field.
         * Only includes files with actual PK/FK data configured.
         * @returns {string} JSON string
         */
        pkFkDataJson() {
            if (!this.showPkFkSection) return '';

            const pkFkData = [];
            this.csvFiles.forEach((file, index) => {
                const pk = this.pkSelections[index] || '';
                const fk = this.fkSelections[index] || '';
                const fkTable = this.fkTableSelections[index] || '';
                const fkColumn = this.fkColumnSelections[index] || '';

                if (pk || fk) {
                    pkFkData.push({
                        fileName: file.name,
                        primaryKey: pk || null,
                        foreignKey: fk || null,
                        foreignKeyTable: fkTable || null,
                        foreignKeyColumn: fkColumn || null
                    });
                }
            });

            return JSON.stringify(pkFkData);
        },

        /**
         * Serialized cross-graph linking data for the hidden form field.
         * @returns {string} JSON string
         */
        crossGraphLinkDataJson() {
            if (!this.enableDataLinking) return '';

            const crossGraphLink = {
                newTableName: this.newTableName,
                newColumnName: this.newColumnName,
                existingTableName: this.existingTableName,
                existingColumnName: this.existingColumnName
            };

            if (crossGraphLink.newTableName && crossGraphLink.newColumnName &&
                crossGraphLink.existingTableName && crossGraphLink.existingColumnName) {
                return JSON.stringify(crossGraphLink);
            }
            return '';
        }
    },

    methods: {
        /**
         * Open the hidden file input dialog.
         */
        triggerFileInput() {
            this.$refs.csvFileInput.click();
        },

        /**
         * Handle file input change event.
         * Reads selected files, updates reactive state, and analyzes CSV columns.
         * @param {Event} event - The change event from the file input
         */
        handleFileChange(event) {
            const input = event.target;
            this.csvFiles = [];
            this.csvColumns = {};
            this.resetPkFkSelections();

            const filePaths = [];
            for (let i = 0; i < input.files.length; i++) {
                filePaths.push(input.files[i].name);
                this.csvFiles.push(input.files[i]);
            }
            this.csvPath = filePaths.join(', ');

            this.analyzeCSVFiles(input.files).then(() => {
                // Show/hide PK/FK section based on number of files
                this.showPkFkSection = input.files.length > 1;

                // Show data linking section when files are uploaded (if graph exists)
                this.showDataLinkingSection = this.graphExists && input.files.length > 0;
            });
        },

        /**
         * Reset all PK/FK selection state to empty defaults.
         */
        resetPkFkSelections() {
            this.pkSelections = {};
            this.fkSelections = {};
            this.fkTableSelections = {};
            this.fkColumnSelections = {};
        },

        /**
         * Analyze CSV files to extract column headers.
         * @param {FileList} files - Files to analyze
         * @returns {Promise<void>}
         */
        analyzeCSVFiles(files) {
            const promises = [];
            for (let i = 0; i < files.length; i++) {
                promises.push(this.readCSVColumns(files[i]));
            }

            return Promise.all(promises).then(results => {
                const newColumns = {};
                results.forEach((columns, index) => {
                    newColumns[files[index].name] = columns;
                });
                this.csvColumns = newColumns;
            }).catch(error => {
                console.error('Error analyzing CSV files:', error);
            });
        },

        /**
         * Read column headers from a CSV file by reading only the first 1KB.
         * @param {File} file - CSV file to read
         * @returns {Promise<string[]>} Resolved with column header names
         */
        readCSVColumns(file) {
            const self = this;
            return new Promise((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = function(e) {
                    try {
                        const csv = e.target.result;
                        const lines = csv.split('\n');
                        if (lines.length > 0) {
                            const separator = self.detectSeparator(lines[0]);
                            const headers = self.parseCSVLine(lines[0], separator);
                            resolve(headers);
                        } else {
                            resolve([]);
                        }
                    } catch (error) {
                        console.error('Error parsing CSV:', error);
                        resolve([]);
                    }
                };
                reader.onerror = reject;
                reader.readAsText(file.slice(0, 1024));
            });
        },

        /**
         * Detect CSV separator character by counting occurrences of common separators.
         * @param {string} line - First line of CSV
         * @returns {string} Detected separator character
         */
        detectSeparator(line) {
            const separators = [',', ';', '\t', '|'];
            let bestSeparator = ',';
            let maxCount = 0;

            for (const sep of separators) {
                const count = line.split(sep).length - 1;
                if (count > maxCount) {
                    maxCount = count;
                    bestSeparator = sep;
                }
            }

            return bestSeparator;
        },

        /**
         * Parse a CSV line respecting quoted fields.
         * @param {string} line - CSV line to parse
         * @param {string} separator - Separator character
         * @returns {string[]} Parsed field values
         */
        parseCSVLine(line, separator) {
            const result = [];
            let current = '';
            let inQuotes = false;

            for (let i = 0; i < line.length; i++) {
                const char = line[i];

                if (char === '"') {
                    inQuotes = !inQuotes;
                } else if (char === separator && !inQuotes) {
                    result.push(current.trim());
                    current = '';
                } else {
                    current += char;
                }
            }

            result.push(current.trim());
            return result.map(h => h.replace(/"/g, '').trim()).filter(h => h.length > 0);
        },

        /**
         * Get column names for a given filename.
         * @param {string} fileName - Name of the CSV file
         * @returns {string[]} Column names, or empty array if not found
         */
        getFileColumns(fileName) {
            if (!fileName) return [];
            return this.csvColumns[fileName] || [];
        },

        /**
         * Get all CSV files except the one with the given name (for FK table references).
         * @param {string} currentFileName - Name of the current file to exclude
         * @returns {Array<File>} Other CSV files
         */
        getOtherFiles(currentFileName) {
            return this.csvFiles.filter(f => f.name !== currentFileName);
        },

        /**
         * Derive a table name from a CSV filename by removing the .csv extension.
         * @param {string} fileName - CSV filename
         * @returns {string} Table name
         */
        tableName(fileName) {
            return fileName.replace('.csv', '');
        },

        /**
         * Handle FK referenced table selection change.
         * Resets the referenced column selection for the given file index.
         * @param {number} index - File index
         */
        onFkTableChange(index) {
            this.fkColumnSelections[index] = '';
        },

        /**
         * Validate PK/FK relationships.
         * Checks that any table referenced by a foreign key has a primary key selected.
         * @returns {boolean} True if all FK relationships are valid
         */
        validatePkFkRelationships() {
            if (!this.showPkFkSection) return true;

            let valid = true;
            this.csvFiles.forEach((file, index) => {
                const fk = this.fkSelections[index] || '';
                const fkTable = this.fkTableSelections[index] || '';

                if (fk && fkTable) {
                    const referencedTableIndex = this.csvFiles.findIndex(f => f.name === fkTable);
                    if (referencedTableIndex !== -1) {
                        const referencedTablePk = this.pkSelections[referencedTableIndex] || '';
                        if (!referencedTablePk) {
                            valid = false;
                        }
                    }
                }
            });

            return valid;
        },

        /**
         * Load existing graph structure from the server via AJAX.
         * Populates the existingGraphStructure data property.
         */
        loadExistingGraphData() {
            const self = this;
            $.ajax({
                url: '/get-existing-graph-structure',
                method: 'GET',
                success(data) {
                    self.existingGraphStructure = data;
                },
                error(xhr, status, error) {
                    console.error('Error loading existing graph structure:', error);
                }
            });
        },

        /**
         * Handle form submission.
         * Starts loading animation on the appropriate button and allows the
         * native form POST to proceed.
         * @param {Event} e - The submit event
         */
        onFormSubmit(e) {
            if (e.submitter && e.submitter.id === 'submitFilesButton') {
                this.startLoadingAnimation('#submitFilesButton');
            }
            // Hidden fields (pkFkData, crossGraphLinkData) are already bound
            // via computed properties - the form submits naturally.
        },

        /**
         * Handle "Proceed without adding data" button click.
         * Submits the form without file data.
         */
        submitWithoutData() {
            this.$el.submit();
        },

        /**
         * Start loading animation on a button.
         * Alternates between cookie and cookie-bite icons with fade transitions.
         * @param {string} buttonSelector - jQuery selector for the button
         */
        startLoadingAnimation(buttonSelector) {
            const button = $(buttonSelector);
            const originalText = button.html();

            button.prop('disabled', true);

            let isCookieBite = false;
            button.html('<i class="fas fa-cookie loading-icon"></i> Processing...');

            const loadingInterval = setInterval(function() {
                const icon = button.find('.loading-icon');
                icon.addClass('icon-fade-out');

                setTimeout(function() {
                    if (isCookieBite) {
                        icon.removeClass('fa-cookie-bite').addClass('fa-cookie');
                    } else {
                        icon.removeClass('fa-cookie').addClass('fa-cookie-bite');
                    }
                    isCookieBite = !isCookieBite;
                    icon.removeClass('icon-fade-out').addClass('icon-fade-in');

                    setTimeout(function() {
                        icon.removeClass('icon-fade-in');
                    }, 300);
                }, 150);
            }, 1000);

            button.data('original-text', originalText);
            button.data('loading-interval', loadingInterval);
        },

        /**
         * Stop loading animation on a button and restore original content.
         * @param {string} buttonSelector - jQuery selector for the button
         */
        stopLoadingAnimation(buttonSelector) {
            const button = $(buttonSelector);
            const loadingInterval = button.data('loading-interval');

            if (loadingInterval) {
                clearInterval(loadingInterval);
            }

            const originalText = button.data('original-text');
            if (originalText) {
                button.html(originalText);
            }
            button.prop('disabled', false);
        },

        /**
         * Escape HTML special characters to prevent XSS.
         * Delegates to StatusMessages.escapeHtml if available, otherwise uses a DOM-based fallback.
         * @param {string} text - Text to escape
         * @returns {string} Escaped HTML string
         */
        escapeHtml(text) {
            if (typeof StatusMessages !== 'undefined' && StatusMessages.escapeHtml) {
                return StatusMessages.escapeHtml(text);
            }
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    },

    /**
     * Lifecycle hook: called after the component is mounted.
     * Loads existing graph data if a graph exists.
     */
    mounted() {
        if (this.graphExists) {
            this.loadExistingGraphData();
        }
    }
});

// Export for global access
window.IngestApp = IngestApp;

/**
 * Global bridge function for backward compatibility.
 * Called from inline onchange handlers if any remain in the HTML.
 * In the Vue app, file input is handled via @change binding instead.
 * @param {HTMLInputElement} input - The file input element
 */
function updateFilePaths(input) {
    // Vue handles this through the @change binding on the file input.
    // This function exists for backward compatibility with any remaining
    // inline onchange="updateFilePaths(this)" handlers in the HTML.
    console.warn(
        'updateFilePaths() called via legacy handler. ' +
        'The Vue ingest-app component handles file input changes internally.'
    );
}

/**
 * Ingest Page Module
 * Handles data ingestion page functionality including CSV upload, PK/FK configuration,
 * and cross-graph data linking.
 */

const IngestPage = {
    csvFiles: [],
    csvColumns: {},
    existingGraphStructure: null,
    graphExists: false,

    /**
     * Initialize the ingest page
     * @param {boolean} graphExists - Whether a graph already exists in GraphDB
     */
    init: function(graphExists) {
        this.graphExists = graphExists || false;
        this.bindEventHandlers();
        this.updateSubmitButtonState();

        if (this.graphExists) {
            this.loadExistingGraphData();
            this.initDataLinking();
        }
    },

    /**
     * Bind all event handlers
     */
    bindEventHandlers: function() {
        const self = this;

        // Initially disable the submit button
        $("#submitFilesButton").prop("disabled", true);

        // Enable the button when necessary fields are filled
        $("input[type=text], input[type=file], input[type=password]").on("change keyup", function() {
            self.updateSubmitButtonState();
        });

        // Add event listeners to PK/FK dropdowns
        $(document).on('change', '[id^="pk_"], [id^="fk_"], [id^="fkTable_"], [id^="fkColumn_"]', function() {
            self.updateSubmitButtonState();
        });

        // Data source selection
        $("#CSV").click(function() {
            $("#csvform").show();
            $("#postgresform").hide();
        });

        $("#Postgres").click(function() {
            $("#csvform").hide();
            $("#postgresform").show();
            $("#dataLinkingSection").hide();
        });

        // Form submission
        $("#mainForm").submit(function(e) {
            if (e.originalEvent && e.originalEvent.submitter && 
                $(e.originalEvent.submitter).attr('id') === 'submitFilesButton') {
                self.startLoadingAnimation('#submitFilesButton');
            }

            // Collect PK/FK data
            if ($("#CSV").is(":checked") && $("#pkFkSection").is(":visible")) {
                const pkFkData = self.collectPkFkData();
                $("#pkFkData").val(JSON.stringify(pkFkData));
            }

            // Collect cross-graph linking data
            if ($("#enableDataLinking").is(':checked')) {
                const crossGraphLink = {
                    newTableName: $('#newTableName').val(),
                    newColumnName: $('#newColumnName').val(),
                    existingTableName: $('#existingTableName').val(),
                    existingColumnName: $('#existingColumnName').val()
                };

                if (crossGraphLink.newTableName && crossGraphLink.newColumnName &&
                    crossGraphLink.existingTableName && crossGraphLink.existingColumnName) {
                    $("#crossGraphLinkData").val(JSON.stringify(crossGraphLink));
                }
            }
        });

        // Proceed without data button
        $("#submitWithoutData").click(function(e) {
            e.preventDefault();
            $("#mainForm").submit();
        });
    },

    /**
     * Update file paths when files are selected
     * @param {HTMLInputElement} input - The file input element
     */
    updateFilePaths: function(input) {
        const self = this;
        this.csvFiles = [];
        this.csvColumns = {};

        let filePaths = [];
        for (let i = 0; i < input.files.length; i++) {
            filePaths.push(input.files[i].name);
            this.csvFiles.push(input.files[i]);
        }
        document.getElementById('csvPath').value = filePaths.join(', ');

        // Analyze CSV files to get column information
        this.analyzeCSVFiles(input.files).then(() => {
            // Show/hide PK/FK section based on number of files
            if (input.files.length > 1) {
                self.generatePkFkFields();
                $("#pkFkSection").slideDown();
            } else {
                $("#pkFkSection").hide();
            }

            // Show data linking section when files are uploaded (if graph exists)
            if (self.graphExists && input.files.length > 0) {
                $("#dataLinkingSection").slideDown();
                self.updateNewTableOptions();
            } else {
                $("#dataLinkingSection").hide();
            }
        });
    },

    /**
     * Load existing graph structure from the server
     */
    loadExistingGraphData: function() {
        const self = this;
        $.ajax({
            url: '/get-existing-graph-structure',
            method: 'GET',
            success: function(data) {
                const existingTableSelect = $('#existingTableName');
                existingTableSelect.empty().append('<option value="">-- Select Existing Table --</option>');

                if (data.tables) {
                    data.tables.forEach(table => {
                        existingTableSelect.append(`<option value="${table}">${table}</option>`);
                    });
                }

                self.existingGraphStructure = data;
            },
            error: function(xhr, status, error) {
                console.error('Error loading existing graph structure:', error);
            }
        });
    },

    /**
     * Initialize data linking functionality
     */
    initDataLinking: function() {
        const self = this;

        $('#enableDataLinking').change(function() {
            if ($(this).is(':checked')) {
                $('#linkingConfiguration').slideDown();
            } else {
                $('#linkingConfiguration').slideUp();
            }
        });

        // Handle new table selection
        $('#newTableName').change(function() {
            const selectedTable = $(this).val();
            const newColumnSelect = $('#newColumnName');
            newColumnSelect.empty().append('<option value="">-- Select New Column --</option>');

            if (selectedTable) {
                const selectedFile = self.csvFiles.find(f => f.name.replace('.csv', '') === selectedTable);
                if (selectedFile && self.csvColumns[selectedFile.name]) {
                    self.csvColumns[selectedFile.name].forEach(column => {
                        newColumnSelect.append(`<option value="${column}">${column}</option>`);
                    });
                }
            }
        });

        // Handle existing table selection
        $('#existingTableName').change(function() {
            const selectedTable = $(this).val();
            const existingColumnSelect = $('#existingColumnName');
            existingColumnSelect.empty().append('<option value="">-- Select Existing Column --</option>');

            if (selectedTable && self.existingGraphStructure && self.existingGraphStructure.tableColumns) {
                const columns = self.existingGraphStructure.tableColumns[selectedTable] || [];
                columns.forEach(column => {
                    existingColumnSelect.append(`<option value="${column}">${column}</option>`);
                });
            }
        });
    },

    /**
     * Analyze CSV files to extract column headers
     * @param {FileList} files - Files to analyze
     * @returns {Promise}
     */
    analyzeCSVFiles: function(files) {
        const self = this;
        let promises = [];

        for (let i = 0; i < files.length; i++) {
            promises.push(this.readCSVColumns(files[i]));
        }

        return Promise.all(promises).then(results => {
            results.forEach((columns, index) => {
                self.csvColumns[files[index].name] = columns;
            });
        }).catch(error => {
            console.error('Error analyzing CSV files:', error);
        });
    },

    /**
     * Read column headers from a CSV file
     * @param {File} file - CSV file to read
     * @returns {Promise<string[]>} Column headers
     */
    readCSVColumns: function(file) {
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
            reader.readAsText(file.slice(0, 1024)); // Read only first 1KB for headers
        });
    },

    /**
     * Detect CSV separator character
     * @param {string} line - First line of CSV
     * @returns {string} Detected separator
     */
    detectSeparator: function(line) {
        const separators = [',', ';', '\t', '|'];
        let bestSeparator = ',';
        let maxCount = 0;

        for (let sep of separators) {
            const count = (line.split(sep).length - 1);
            if (count > maxCount) {
                maxCount = count;
                bestSeparator = sep;
            }
        }

        return bestSeparator;
    },

    /**
     * Parse a CSV line respecting quoted fields
     * @param {string} line - CSV line to parse
     * @param {string} separator - Separator character
     * @returns {string[]} Parsed fields
     */
    parseCSVLine: function(line, separator) {
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
     * Generate PK/FK configuration fields for each CSV file
     */
    generatePkFkFields: function() {
        const self = this;
        let content = '';

        this.csvFiles.forEach((file, index) => {
            const fileName = file.name;
            const columns = this.csvColumns[fileName] || [];

            content += `
                <div class="card mb-3">
                    <div class="card-header bg-light">
                        <h6 class="mb-0">
                            <i class="fas fa-table"></i> ${fileName}
                            <small class="text-muted">(${columns.length} columns detected)</small>
                        </h6>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="form-group">
                                    <label for="pk_${index}" class="font-weight-bold">
                                        <i class="fas fa-key text-warning"></i> Primary Key:
                                    </label>
                                    <select class="form-control" id="pk_${index}" name="pk_${index}">
                                        <option value="">-- No Primary Key --</option>
                                        ${columns.map(col => `<option value="${col}">${col}</option>`).join('')}
                                    </select>
                                    <small class="form-text text-muted">
                                        Column that uniquely identifies each row in this file
                                    </small>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="form-group">
                                    <label for="fk_${index}" class="font-weight-bold">
                                        <i class="fas fa-link text-info"></i> Foreign Key:
                                    </label>
                                    <select class="form-control" id="fk_${index}" name="fk_${index}">
                                        <option value="">-- No Foreign Key --</option>
                                        ${columns.map(col => `<option value="${col}">${col}</option>`).join('')}
                                    </select>
                                    <small class="form-text text-muted">
                                        Column that references another table's primary key
                                    </small>
                                </div>
                            </div>
                        </div>
                        <div class="row" id="fkDetails_${index}" style="display: none;">
                            <div class="col-md-6">
                                <div class="form-group">
                                    <label for="fkTable_${index}" class="font-weight-bold">
                                        <i class="fas fa-arrow-right text-success"></i> References Table:
                                    </label>
                                    <select class="form-control" id="fkTable_${index}" name="fkTable_${index}">
                                        <option value="">-- Select Referenced Table --</option>
                                        ${this.csvFiles.map(f => f.name !== fileName ? `<option value="${f.name}">${f.name}</option>` : '').join('')}
                                    </select>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="form-group">
                                    <label for="fkColumn_${index}" class="font-weight-bold">
                                        <i class="fas fa-arrow-right text-success"></i> References Column:
                                    </label>
                                    <select class="form-control" id="fkColumn_${index}" name="fkColumn_${index}">
                                        <option value="">-- Select Referenced Column --</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });

        content += `
            <div class="alert alert-light border">
                <h6><i class="fas fa-lightbulb text-warning"></i> Tips:</h6>
                <ul class="mb-0 small">
                    <li><strong>Primary Key:</strong> A column that uniquely identifies each row (e.g., ID, PatientID)</li>
                    <li><strong>Foreign Key:</strong> A column that references a primary key in another table</li>
                    <li>Leave fields empty if no relationships exist</li>
                    <li>These relationships will be used to create connections in your knowledge graph</li>
                </ul>
            </div>
        `;

        document.getElementById('pkFkContent').innerHTML = content;

        // Add event listeners for FK changes
        this.csvFiles.forEach((file, index) => {
            $(`#fk_${index}`).change(function() {
                if ($(this).val()) {
                    $(`#fkDetails_${index}`).slideDown();
                } else {
                    $(`#fkDetails_${index}`).slideUp();
                }
            });

            $(`#fkTable_${index}`).change(function() {
                const selectedTable = $(this).val();
                const targetColumns = self.csvColumns[selectedTable] || [];

                $(`#fkColumn_${index}`).html(
                    '<option value="">-- Select Referenced Column --</option>' +
                    targetColumns.map(col => `<option value="${col}">${col}</option>`).join('')
                );
            });
        });
    },

    /**
     * Collect PK/FK configuration data
     * @returns {Object[]} PK/FK data for each file (only includes files with actual PK/FK data)
     */
    collectPkFkData: function() {
        let pkFkData = [];

        this.csvFiles.forEach((file, index) => {
            const pk = $(`#pk_${index}`).val();
            const fk = $(`#fk_${index}`).val();
            const fkTable = $(`#fkTable_${index}`).val();
            const fkColumn = $(`#fkColumn_${index}`).val();

            // Only include files that have actual PK/FK data configured
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

        return pkFkData;
    },

    /**
     * Update new table dropdown options
     */
    updateNewTableOptions: function() {
        const newTableSelect = $('#newTableName');
        newTableSelect.empty().append('<option value="">-- Select New Table --</option>');

        this.csvFiles.forEach(file => {
            const tableName = file.name.replace('.csv', '');
            newTableSelect.append(`<option value="${tableName}">${tableName}</option>`);
        });
    },

    /**
     * Validate PK/FK relationships
     * @returns {boolean} True if valid
     */
    validatePkFkRelationships: function() {
        if (!$("#pkFkSection").is(":visible")) {
            return true;
        }

        let hasIncompleteFkRelationship = false;

        this.csvFiles.forEach((file, index) => {
            const fk = $(`#fk_${index}`).val();
            const fkTable = $(`#fkTable_${index}`).val();

            if (fk && fkTable) {
                const referencedTableIndex = this.csvFiles.findIndex(f => f.name === fkTable);
                if (referencedTableIndex !== -1) {
                    const referencedTablePk = $(`#pk_${referencedTableIndex}`).val();
                    if (!referencedTablePk) {
                        hasIncompleteFkRelationship = true;
                    }
                }
            }
        });

        return !hasIncompleteFkRelationship;
    },

    /**
     * Update submit button state based on form validation
     */
    updateSubmitButtonState: function() {
        const csvFile = $("#csvFile").val();
        const username = $("#username").val();
        const password = $("#password").val();
        const postgresUrl = $("#POSTGRES_URL").val();
        const postgresDb = $("#POSTGRES_DB").val();

        const basicValidation = ($("#CSV").is(":checked") && csvFile) ||
                               ($("#Postgres").is(":checked") && username && password && postgresUrl && postgresDb);

        const pkFkValidation = this.validatePkFkRelationships();

        if (basicValidation && pkFkValidation) {
            $("#submitFilesButton").prop("disabled", false);
            $("#submitFilesButton").removeAttr("title");
        } else {
            $("#submitFilesButton").prop("disabled", true);
            if (!pkFkValidation) {
                $("#submitFilesButton").attr("title", "Please select primary keys for all tables that are referenced by foreign keys");
            }
        }
    },

    /**
     * Start loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    startLoadingAnimation: function(buttonSelector) {
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
     * Stop loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    stopLoadingAnimation: function(buttonSelector) {
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
    }
};

// Global function for file input onchange
function updateFilePaths(input) {
    IngestPage.updateFilePaths(input);
}

// Export for global access
window.IngestPage = IngestPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = IngestPage;
}

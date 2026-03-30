/**
 * Annotation Landing Page Module
 * Handles the annotation landing page functionality including JSON-LD upload
 * and IndexedDB semantic map detection.
 */

const AnnotationLandingPage = {
    graphDbDatabases: [],

    /**
     * Initialize the annotation landing page
     */
    init: function() {
        this.checkIndexedDbSemanticMap();
        this.bindEventHandlers();
    },

    /**
     * Bind all event handlers
     */
    bindEventHandlers: function() {
        const self = this;

        // Handle JSON-LD upload form submission
        $("#jsonUploadForm").submit(async function(e) {
            e.preventDefault();
            await self.handleJsonUpload();
        });
    },

    /**
     * Update annotation JSON path display
     * @param {HTMLInputElement} input - The file input element
     */
    updateAnnotationJsonPath: function(input) {
        const fullPath = input.value;
        const fileName = fullPath.split('\\').pop();
        $("#annotationJsonPath").val(fileName);
        $("#uploadJsonButton").prop("disabled", !fileName);
    },

    /**
     * Show status message at the top of the page
     * @param {string} message - Message to display
     * @param {string} type - Message type: 'success', 'error', 'info'
     */
    showStatusMessage: function(message, type) {
        type = type || 'success';
        
        const alertClass = type === "error" ? "alert-danger" :
            type === "info" ? "alert-info" : "alert-success";

        const iconClass = type === "error" ? "exclamation-triangle" :
            type === "info" ? "info-circle" : "check-circle";

        const alertHtml = `
            <div class="alert ${alertClass} alert-dismissible fade show" role="alert">
                <i class="fas fa-${iconClass}"></i>
                ${message}
                <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>
            </div>
        `;

        $("#statusMessages").html(alertHtml);
    },

    /**
     * Show upload status message inside the Option 1 card
     * @param {string} message - Message to display
     * @param {string} type - Message type
     */
    showUploadStatus: function(message, type) {
        type = type || 'success';
        
        const alertClass = type === "error" ? "alert-danger" :
            type === "info" ? "alert-info" : "alert-success";
        const iconClass = type === "error" ? "exclamation-triangle" :
            type === "info" ? "info-circle" : "check-circle";

        const alertHtml = `
            <div class="alert ${alertClass}" style="font-size: 0.9em;">
                <i class="fas fa-${iconClass}"></i>
                ${message}
            </div>
        `;

        $("#uploadStatusMessage").html(alertHtml).show();
    },

    /**
     * Escape HTML to prevent XSS
     * @param {string} text - Text to escape
     * @returns {string} Escaped text
     */
    escapeHtml: function(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    },

    /**
     * Check if a database name matches any GraphDB database
     * @param {string} mapDbName - Database name from semantic map
     * @param {string[]} graphDbList - List of GraphDB databases
     * @returns {string|null} Matching database or null
     */
    findMatchingDatabase: function(mapDbName, graphDbList) {
        if (!mapDbName || mapDbName === '') return null;

        for (const db of graphDbList) {
            if (db === mapDbName) return db;

            const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
            const dbNoExt = db.endsWith('.csv') ? db.slice(0, -4) : db;
            if (mapNoExt === dbNoExt) return db;
        }
        return null;
    },

    /**
     * Extract table names from JSON-LD structure
     * @param {Object} data - JSON-LD data
     * @returns {string[]} Table names
     */
    extractJsonLdTables: function(data) {
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
     * Generate database comparison HTML
     * @param {string[]} jsonldTables - Tables from JSON-LD
     * @param {string[]} graphDbList - Databases in GraphDB
     * @returns {Object} Comparison result
     */
    generateDatabaseComparisonHtml: function(jsonldTables, graphDbList) {
        const self = this;
        const matching = [];
        const nonMatchingJsonld = [];
        const nonMatchingGraphDb = [...graphDbList];

        for (const jsonldTable of jsonldTables) {
            const match = this.findMatchingDatabase(jsonldTable, graphDbList);
            if (match) {
                matching.push({ jsonld: jsonldTable, graphdb: match });
                const idx = nonMatchingGraphDb.indexOf(match);
                if (idx > -1) nonMatchingGraphDb.splice(idx, 1);
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
                <i class="fas fa-exclamation-triangle"></i> <strong>Not in GraphDB:</strong> ${nonMatchingJsonld.map(t => self.escapeHtml(t)).join(', ')}
            </div>`;
        }

        if (nonMatchingGraphDb.length > 0) {
            html += `<div class="text-muted mb-2" style="font-size: 0.9em;">
                <i class="fas fa-info-circle"></i> <strong>Other data in GraphDB:</strong> ${nonMatchingGraphDb.map(t => self.escapeHtml(t)).join(', ')}
            </div>`;
        }

        return {
            html: html,
            hasMatches: matching.length > 0,
            matching: matching,
            nonMatchingJsonld: nonMatchingJsonld,
            nonMatchingGraphDb: nonMatchingGraphDb
        };
    },

    /**
     * Fetch databases from GraphDB and store in IndexedDB
     * @returns {Promise<string[]>} List of databases
     */
    fetchAndStoreGraphDbDatabases: async function() {
        try {
            const response = await fetch('/api/graphdb-databases');
            const data = await response.json();

            if (data.success && data.databases && data.databases.length > 0) {
                console.log('Fetched GraphDB databases:', data.databases);
                this.graphDbDatabases = data.databases;

                await FlyoverDB.saveData('metadata', {
                    key: 'graphdb_databases',
                    data: data.databases,
                    timestamp: new Date().toISOString()
                });

                return data.databases;
            } else {
                console.warn('No databases found in GraphDB:', data.message);
                return [];
            }
        } catch (error) {
            console.error('Error fetching GraphDB databases:', error);
            return [];
        }
    },

    /**
     * Check IndexedDB for existing semantic map and compare with GraphDB databases
     */
    checkIndexedDbSemanticMap: async function() {
        try {
            if (typeof FlyoverDB === 'undefined') {
                console.warn('FlyoverDB not available');
                return;
            }

            await FlyoverDB.initDB();
            this.graphDbDatabases = await this.fetchAndStoreGraphDbDatabases();

            const result = await FlyoverDB.getData('metadata', 'semantic_map');

            if (result && result.data) {
                console.log('IndexedDB: Found semantic map', result);

                document.getElementById('indexedDbSection').style.display = 'block';

                const jsonldTables = this.extractJsonLdTables(result.data);
                const comparison = this.generateDatabaseComparisonHtml(jsonldTables, this.graphDbDatabases);
                document.getElementById('indexedDbMatchInfo').innerHTML = comparison.html;

                if (comparison.hasMatches) {
                    document.getElementById('continueToReviewBtn').style.display = 'inline-block';
                    document.getElementById('indexedDbErrorMessage').style.display = 'none';
                } else {
                    document.getElementById('continueToReviewBtn').style.display = 'none';
                    document.getElementById('indexedDbErrorMessage').style.display = 'block';
                    document.getElementById('indexedDbErrorMessage').innerHTML = `
                        <div class="alert alert-danger mt-3" style="font-size: 0.9em;">
                            <i class="fas fa-times-circle"></i>
                            <strong>Cannot proceed:</strong> No matching data sources found between your semantic map and GraphDB.
                        </div>`;
                }

                document.getElementById('existingJsonldWarning').style.display = 'block';
                document.getElementById('existingJsonldDatabases').textContent = jsonldTables.join(', ') || 'Unknown';
            } else {
                console.log('IndexedDB: No semantic map found');
                document.getElementById('existingJsonldWarning').style.display = 'none';
            }
        } catch (error) {
            console.error('Error checking IndexedDB for semantic map:', error);
        }
    },

    /**
     * Read file as text
     * @param {File} file - File to read
     * @returns {Promise<string>} File contents
     */
    readFileAsText: function(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => resolve(e.target.result);
            reader.onerror = (e) => reject(new Error('Failed to read file'));
            reader.readAsText(file);
        });
    },

    /**
     * Handle JSON-LD file upload
     */
    handleJsonUpload: async function() {
        const self = this;

        if (!$("#annotationJsonFile")[0].files.length) {
            this.showStatusMessage("Please select a JSON-LD file to upload.", "error");
            return;
        }

        const file = $("#annotationJsonFile")[0].files[0];

        $("#uploadJsonButton").prop("disabled", true).html('<i class="fas fa-spinner fa-spin"></i> Uploading...');
        $("#uploadStatusMessage").hide();

        try {
            const fileContent = await this.readFileAsText(file);
            let jsonData;

            try {
                jsonData = JSON.parse(fileContent);
            } catch (parseError) {
                this.showUploadStatus("Invalid JSON-LD file format. Please check your file.", "error");
                $("#uploadJsonButton").prop("disabled", false).html('<i class="fas fa-upload"></i> Upload JSON-LD for Annotation');
                return;
            }

            const formData = new FormData();
            formData.append("annotationJsonFile", file);

            const response = await fetch("/upload-annotation-json", {
                method: "POST",
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

                let message = "<strong>JSON-LD file uploaded successfully!</strong><br>";

                if (result.matching_databases && result.matching_databases.length > 0) {
                    message += `<i class="fas fa-check-circle text-success"></i> ${result.matching_databases.length} data source(s) matched and ready for annotation.<br>`;
                }

                if (result.non_matching_jsonld && result.non_matching_jsonld.length > 0) {
                    message += `<i class="fas fa-exclamation-triangle text-warning"></i> ${result.non_matching_jsonld.length} data source(s) in semantic map not found in GraphDB.<br>`;
                }

                this.showUploadStatus(message, "success");

                $("#uploadInfoSection").hide();
                $("#existingJsonldWarning").hide();
                $("#indexedDbSection").hide();

                $("#uploadJsonButton")
                    .prop("disabled", false)
                    .removeClass("btn-success")
                    .addClass("btn-primary")
                    .html('<i class="fas fa-arrow-right"></i> Proceed to Annotation Review')
                    .off("click")
                    .on("click", function(e) {
                        e.preventDefault();
                        window.location.href = "/annotation-review";
                    });
            } else {
                let errorMessage = result.error || "Failed to upload JSON-LD file.";

                if (result.graphdb_databases && result.graphdb_databases.length > 0) {
                    errorMessage += "<br><br><strong>Data available in GraphDB:</strong><ul>";
                    result.graphdb_databases.forEach(db => {
                        errorMessage += `<li>${self.escapeHtml(db)}</li>`;
                    });
                    errorMessage += "</ul>";
                }

                if (result.jsonld_databases && result.jsonld_databases.length > 0) {
                    errorMessage += "<strong>Data sources in uploaded semantic map:</strong><ul>";
                    result.jsonld_databases.forEach(db => {
                        errorMessage += `<li>${self.escapeHtml(db)}</li>`;
                    });
                    errorMessage += "</ul>";
                }

                this.showUploadStatus(errorMessage, "error");
                $("#uploadJsonButton").prop("disabled", false).html('<i class="fas fa-upload"></i> Upload JSON-LD for Annotation');
            }
        } catch (error) {
            console.error('Upload error:', error);
            this.showUploadStatus("Failed to upload file: " + error.message, "error");
            $("#uploadJsonButton").prop("disabled", false).html('<i class="fas fa-upload"></i> Upload JSON-LD for Annotation');
        }
    }
};

// Global function for template event handlers
function updateAnnotationJsonPath(input) {
    AnnotationLandingPage.updateAnnotationJsonPath(input);
}

// Initialize on DOM ready
$(document).ready(function() {
    AnnotationLandingPage.init();
});

// Export for global access
window.AnnotationLandingPage = AnnotationLandingPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AnnotationLandingPage;
}

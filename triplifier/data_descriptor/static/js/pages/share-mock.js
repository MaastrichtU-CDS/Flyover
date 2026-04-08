/**
 * Share Mock Data Page Module
 * Handles the mock data generation page functionality.
 */

const ShareMockPage = {
    // Global variables
    storedJsonldData: null,
    detectedJsonldData: null,
    generatedData: null,

    /**
     * Initialize the share mock page
     */
    init: function() {
        console.log('Flyover: Initializing Share Mock page');
        this.setupEventHandlers();
        this.loadDataFromIndexedDB();
    },

    /**
     * Set up event handlers for the page
     */
    setupEventHandlers: function() {
        const self = this;

        // File upload handling
        $('#jsonldFileInput').change(function (e) {
            const file = e.target.files[0];
            if (file) {
                $('.custom-file-label').html(file.name);
                $('#uploadJsonldBtn').prop('disabled', false);
            } else {
                $('.custom-file-label').html('Choose JSON-LD file...');
                $('#uploadJsonldBtn').prop('disabled', true);
            }
        });

        $('#uploadJsonldBtn').click(async function () {
            await self.handleFileUpload();
        });

        $('#useStoredJsonldBtn').click(function () {
            if (self.detectedJsonldData) {
                self.storedJsonldData = self.detectedJsonldData;
                // Clean up the warning when using the stored map
                $('#existingJsonldWarning').hide();
                self.updateUIBasedOnJsonldAvailability();
            }
        });

        // Database select change handler
        $('#databaseSelect').change(function () {
            const selectedDb = $(this).val();
            self.updateTableOptions(selectedDb);
        });

        $('#generateMockDataBtn').click(async function () {
            await self.generateMockData();
        });
    },

    /**
     * Handle file upload
     */
    handleFileUpload: async function() {
        const file = $('#jsonldFileInput')[0].files[0];
        if (!file) return;

        // Show a progress bar
        $('#uploadProgress').removeClass('d-none');
        const progressBar = $('#uploadProgress .progress-bar');
        progressBar.width('25%').text('25%');

        try {
            const reader = new FileReader();
            reader.onload = async function (e) {
                try {
                    progressBar.width('50%').text('50%');

                    const fileContent = e.target.result;
                    const mappingData = JSON.parse(fileContent);

                    progressBar.width('75%').text('75%');

                    // Validate the JSON-LD structure
                    if (mappingData && (mappingData.databases || mappingData.variable_info)) {
                        // Store in IndexedDB using the correct method
                        await FlyoverDB.initDB();
                        await FlyoverDB.saveData('metadata', {
                            key: 'semantic_map',
                            data: mappingData,
                            timestamp: new Date().toISOString()
                        });

                        progressBar.width('100%').text('100%');

                        // Update state
                        self.storedJsonldData = mappingData;

                        setTimeout(() => {
                            $('#uploadProgress').addClass('d-none');
                            self.updateUIBasedOnJsonldAvailability();
                        }, 500);
                    } else {
                        throw new Error('Invalid JSON-LD structure: missing databases or variable_info');
                    }
                } catch (error) {
                    console.error('Error processing JSON-LD file:', error);
                    progressBar.addClass('bg-danger').removeClass('bg-success').text('Error');
                    setTimeout(() => {
                        $('#uploadProgress').addClass('d-none');
                        alert('Error processing JSON-LD file: ' + error.message);
                    }, 1000);
                }
            };
            reader.readAsText(file);

        } catch (error) {
            console.error('Error reading file:', error);
            progressBar.addClass('bg-danger').removeClass('bg-success').text('Error');
            setTimeout(() => {
                $('#uploadProgress').addClass('d-none');
                alert('Error reading file: ' + error.message);
            }, 1000);
        }
    },

    /**
     * Load data from IndexedDB
     */
    loadDataFromIndexedDB: async function() {
        try {
            if (typeof FlyoverDB === 'undefined') {
                console.warn('FlyoverDB not available');
                return;
            }

            await FlyoverDB.initDB();
            const result = await FlyoverDB.getData('metadata', 'semantic_map');

            if (result && result.data) {
                this.detectedJsonldData = result.data;
                
                // Show warning and populate info
                $('#existingJsonldWarning').show();
                
                const stats = SharedChecks.getSemanticMapStats(result.data);
                const infoText = stats.dbCount + ' ' + (stats.dbCount === 1 ? 'database' : 'databases') + ', ' +
                               stats.tableCount + ' ' + (stats.tableCount === 1 ? 'table' : 'tables') + ', ' + 'and ' +
                               stats.columnCount + ' ' + (stats.columnCount === 1 ? 'column' : 'columns') + '.' ;
                $('#existingJsonldInfo').text(infoText);
                
                $('#useStoredJsonldBtn').show();
            }
            // If no map, just show the upload form normally (no warning)
            this.updateUIBasedOnJsonldAvailability();
        } catch (error) {
            console.error('Error checking semantic map:', error);
            this.updateUIBasedOnJsonldAvailability();
        }
    },

    /**
     * Populate database and table selects
     */
    populateDatabaseTableSelects: function() {
        console.log('Flyover: Populating database and table selects');
        if (!this.storedJsonldData) return;

        const databases = this.storedJsonldData.databases || {};
        const databaseSelect = $('#databaseSelect');
        const tableSelect = $('#tableSelect');

        // Clear existing options (keep the "All" option)
        databaseSelect.find('option:not(:first)').remove();
        tableSelect.find('option:not(:first)').remove();

        // Populate databases
        Object.keys(databases).forEach(dbId => {
            databaseSelect.append('<option value="' + dbId + '">' + dbId + '</option>');
        });

        // If only one database, auto-select it
        if (Object.keys(databases).length === 1) {
            databaseSelect.val(Object.keys(databases)[0]);
            this.updateTableOptions(Object.keys(databases)[0]);
        }
    },

    /**
     * Update table options based on selected database
     */
    updateTableOptions: function(databaseId) {
        if (!this.storedJsonldData || !databaseId) {
            $('#tableSelect').find('option:not(:first)').remove();
            return;
        }

        const database = this.storedJsonldData.databases[databaseId];
        if (!database || !database.tables) return;

        const tableSelect = $('#tableSelect');
        tableSelect.find('option:not(:first)').remove();

        Object.keys(database.tables).forEach(tableId => {
            tableSelect.append('<option value="' + tableId + '">' + tableId + '</option>');
        });
    },

    /**
     * Update UI based on JSON-LD availability
     */
    updateUIBasedOnJsonldAvailability: function() {
        if (this.storedJsonldData) {
            // Hide all other notifications
            $('#jsonldStatus').addClass('d-none');
            $('#existingJsonldWarning').hide();
            $('#jsonldUploadForm').addClass('d-none');
            
            // Show a success message
            $('#jsonldSuccess').removeClass('d-none');

            // Update the success message with statistics
            const stats = SharedChecks.getSemanticMapStats(this.storedJsonldData);

            // Use proper pluralization
            const dbLabel = stats.dbCount === 1 ? 'database' : 'databases';
            const tableLabel = stats.tableCount === 1 ? 'table' : 'tables';
            const columnLabel = stats.columnCount === 1 ? 'column' : 'columns';

            $('#jsonldSuccessMessage').text(' (' + stats.dbCount + ' ' + dbLabel + ', ' + stats.tableCount + ' ' + tableLabel + ', ' + stats.columnCount + ' ' + columnLabel + ')');

            // Show generation options
            $('#generationOptions').removeClass('d-none');

            // Populate database/table selects
            this.populateDatabaseTableSelects();

        } else {
            // Hide success and generation sections
            $('#jsonldSuccess').addClass('d-none');
            $('#generationOptions').addClass('d-none');
            $('#generationResults').addClass('d-none');

            if (this.detectedJsonldData) {
                // Show warning and an upload form for the detected but not active map
                $('#existingJsonldWarning').show();
                $('#jsonldStatus').addClass('d-none');
                $('#jsonldUploadForm').removeClass('d-none');
                $('#jsonldStatusMessage').text('Existing semantic map detected. You can use it or upload a new one.');
            } else {
                // No map at all - show upload form and info message
                $('#existingJsonldWarning').hide();
                $('#jsonldStatus').removeClass('d-none');
                $('#jsonldUploadForm').removeClass('d-none');
            }
        }
    },

    /**
     * Generate mock data
     */
    generateMockData: async function() {
        if (!this.storedJsonldData) {
            alert('Please upload or select a JSON-LD semantic map first.');
            return;
        }

        const sampleCount = parseInt($('#sampleCount').val());
        const randomSeed = $('#randomSeed').val();
        const databaseId = $('#databaseSelect').val() || null;
        const tableId = $('#tableSelect').val() || null;

        if (isNaN(sampleCount) || sampleCount < 1 || sampleCount > 10000) {
            alert('Please enter a valid number of samples (1-10,000).');
            return;
        }

        // Clean up notifications before starting generation
        $('#jsonldSuccess').addClass('d-none');
        $('#existingJsonldWarning').hide();

        // Show generation progress
        $('#generationProgress').removeClass('d-none');
        const progressBar = $('#generationProgress .progress-bar');
        progressBar.width('25%').text('Preparing generation...');

        try {
            // Call the actual backend API to generate mock data
            const response = await fetch('/api/generate-mock-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    jsonld_map: this.storedJsonldData,
                    num_rows: sampleCount,
                    random_seed: randomSeed ? parseInt(randomSeed) : null,
                    database_id: databaseId,
                    table_id: tableId
                })
            });

            if (!response.ok) {
                throw new Error('Server error: ' + response.status + ' ' + response.statusText);
            }

            const result = await response.json();

            if (!result.success) {
                throw new Error(result.error || 'Failed to generate mock data');
            }

            progressBar.width('75%').text('Processing results...');

            // Store generated data
            this.generatedData = result.data;

            // Store self reference for use in callback
            const self = this;
            
            // Show results
            setTimeout(() => {
                progressBar.width('100%').text('Complete!');
                self.showGenerationResults(result.data);
            }, 500);

        } catch (error) {
            console.error('Error generating mock data:', error);
            progressBar.addClass('bg-danger').removeClass('bg-success').text('Error');
            setTimeout(() => {
                $('#generationProgress').addClass('d-none');
                alert('Error generating mock data: ' + error.message);
            }, 1000);
        }
    },

    /**
     * Show generation results
     */
    showGenerationResults: function(data) {
        // Hide generation options, show results
        $('#generationOptions').addClass('d-none');
        $('#generationResults').removeClass('d-none');
        $('#generationProgress').addClass('d-none');

        // Store the generated data
        this.generatedData = data;

        // Update results message
        const tableCount = Object.keys(data).length;
        const sampleCount = $('#sampleCount').val();

        let details = tableCount + ' table(s) generated with ' + sampleCount + ' samples each.';

        if ($('#databaseSelect').val()) {
            details += ' Database: ' + $('#databaseSelect').val();
        }
        if ($('#tableSelect').val()) {
            details += ' Table: ' + $('#tableSelect').val();
        }

        $('#resultsDetails').text(details);

        // Create download buttons
        const downloadButtons = $('#downloadButtons');
        downloadButtons.empty();

        // Sort table keys for consistent ordering
        const sortedEntries = Object.entries(data).sort(([a], [b]) => a.localeCompare(b));


        // Add a "Download All as ZIP" button first (if multiple tables)
        if (Object.keys(data).length > 1) {
            const zipButton = $(
                '<button class="btn btn-primary download-all-btn zip-btn-width mb-3" id="downloadAllZipBtn">' +
                '    <i class="fas fa-file-archive"></i> Download all as ZIP' +
                '</button>'
            );
            downloadButtons.append(zipButton);

            // Add a line break after the ZIP button
            downloadButtons.append($('<div class="w-100 mt-2"></div>'));
        }
        // Add individual download buttons on the line below
        sortedEntries.forEach(([tableKey, tableData]) => {
            const [dbId, tableId] = tableKey.split('.');
            const button = $(
                '<button class="btn btn-outline-secondary download-btn btn-fixed-width" ' +
                '        data-db="' + dbId + '" ' +
                '        data-table="' + tableId + '" ' +
                '        title="Download ' + dbId + '.' + tableId + ' as CSV">' +
                '    <i class="fas fa-download"></i> ' + dbId + '<br>' + tableId +
                '</button>'
            );
            downloadButtons.append(button);
        });

        // Add event handlers to download buttons
        const self = this; // Store reference for use in callbacks
        $('.download-btn').click(function () {
            const dbId = $(this).data('db');
            const tableId = $(this).data('table');
            const tableKey = dbId + '.' + tableId;

            self.downloadMockDataAsCSV(self.generatedData[tableKey], dbId + '_' + tableId + '_mock.csv');
        });

        // Add event handler for ZIP download
        $('#downloadAllZipBtn').click(async function () {
            await self.downloadAllMockDataAsZIP(self.generatedData);
        });
    },

    /**
     * Download mock data as CSV
     */
    downloadMockDataAsCSV: function(data, filename) {
        try {
            // Convert the mock data to CSV format
            let csvContent = 'data:text/csv;charset=utf-8,';

            // Add headers
            const headers = Object.keys(data[0] || {});
            csvContent += headers.join(',') + '\r\n';

            // Add data rows
            data.forEach(row => {
                const rowData = headers.map(header => {
                    // Handle different data types and escaping
                    let value = row[header];
                    if (value === null || value === undefined) {
                        return '';
                    }

                    // Escape quotes and wrap in quotes if contains commas
                    value = String(value).replace(/"/g, '""');
                    if (value.includes(',')) {
                        value = '"' + value + '"';
                    }
                    return value;
                });
                csvContent += rowData.join(',') + '\r\n';
            });

            // Create a download link
            const encodedUri = encodeURI(csvContent);
            const link = document.createElement('a');
            link.setAttribute('href', encodedUri);
            link.setAttribute('download', filename);
            document.body.appendChild(link);

            // Trigger download
            link.click();
            document.body.removeChild(link);

        } catch (error) {
            console.error('Error generating CSV:', error);
            alert('Error generating CSV download: ' + error.message);
        }
    },

    /**
     * Download all mock data as a ZIP file
     */
    downloadAllMockDataAsZIP: async function(data) {
        try {
            // Use JSZip library
            if (typeof JSZip === 'undefined') {
                console.error('Flyover: JSZip library not loaded');
                alert('ZIP download requires JSZip library. Please try individual downloads.');
                return;
            }

            const zip = new JSZip();

            // Add each table as a separate CSV file in the ZIP
            const sortedEntries = Object.entries(data).sort(([a], [b]) => a.localeCompare(b));

            for (const [tableKey, tableData] of sortedEntries) {
                const [dbId, tableId] = tableKey.split('.');
                const filename = dbId + '_' + tableId + '_mock.csv';

                // Convert table data to CSV format
                let csvContent = '';

                // Add headers
                const headers = Object.keys(tableData[0] || {});
                csvContent += headers.join(',') + '\r\n';

                // Add data rows
                tableData.forEach(row => {
                    const rowData = headers.map(header => {
                        let value = row[header];
                        if (value === null || value === undefined) {
                            return '';
                        }

                        // Escape quotes and wrap in quotes if contains commas
                        value = String(value).replace(/"/g, '""');
                        if (value.includes(',')) {
                            value = '"' + value + '"';
                        }
                        return value;
                    });
                    csvContent += rowData.join(',') + '\r\n';
                });

                zip.file(filename, csvContent);
            }

            // Generate ZIP file
            const zipContent = await zip.generateAsync({type: 'blob'});

            // Create a download link
            const url = URL.createObjectURL(zipContent);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'mock_data_export.zip';
            document.body.appendChild(link);

            // Trigger download
            link.click();
            document.body.removeChild(link);

            // Clean up
            setTimeout(() => URL.revokeObjectURL(url), 100);

        } catch (error) {
            console.error('Flyover: Error creating ZIP file:', error);
            alert('Error creating ZIP download: ' + error.message);
        }
    }
};

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    ShareMockPage.init();
});

// Export for global access
window.ShareMockPage = ShareMockPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ShareMockPage;
}
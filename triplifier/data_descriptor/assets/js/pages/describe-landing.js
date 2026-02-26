/**
 * Describe Landing Page Module
 * Handles the describe landing page functionality including semantic map upload
 * and data existence checks.
 */

const DescribeLandingPage = {
    /**
     * Initialize the describe landing page
     */
    init: function() {
        this.checkDataExists();
        this.bindEventHandlers();
    },

    /**
     * Check if data exists in GraphDB
     */
    checkDataExists: function() {
        const self = this;

        $.ajax({
            url: '/api/check-graph-exists',
            type: 'GET',
            success: function(response) {
                if (!response.exists) {
                    self.showNoDataWarning();
                    $("#continueButton").prop("disabled", true);
                }
            },
            error: function() {
                self.showDataVerificationError();
            }
        });
    },

    /**
     * Show warning when no data is found
     */
    showNoDataWarning: function() {
        $("#dataCheckSection").html(`
            <div class="alert alert-warning" role="alert">
                <i class="fas fa-exclamation-triangle"></i>
                <strong>No Data Found</strong><br>
                You cannot proceed with the describe step as no data has been uploaded yet.
                Please go back to the Ingest step to upload your data first.
                <br><br>
                <a href="/ingest" class="btn btn-primary">
                    <i class="fas fa-arrow-left"></i> Go to Ingest Step
                </a>
            </div>
        `);
    },

    /**
     * Show error when data verification fails
     */
    showDataVerificationError: function() {
        $("#dataCheckSection").html(`
            <div class="alert alert-warning" role="alert">
                <i class="fas fa-exclamation-triangle"></i>
                <strong>Unable to Verify Data</strong><br>
                Please ensure you have completed the Ingest step before proceeding.
                <br><br>
                <a href="/ingest" class="btn btn-primary">
                    <i class="fas fa-arrow-left"></i> Go to Ingest Step
                </a>
            </div>
        `);
    },

    /**
     * Bind all event handlers
     */
    bindEventHandlers: function() {
        const self = this;

        // Handle semantic map form submission
        $("#semanticMapForm").submit(async function(e) {
            e.preventDefault();
            await self.handleSemanticMapUpload();
        });

        // Handle continue form submission
        $("#continueForm").submit(function(e) {
            self.startLoadingAnimation('#continueButton');
        });
    },

    /**
     * Update the semantic map path display
     * @param {HTMLInputElement} input - The file input element
     */
    updateSemanticMapPath: function(input) {
        const fullPath = input.value;
        const fileName = fullPath.split('\\').pop();
        $("#semanticMapPath").val(fileName);
        $("#uploadSemanticMapButton").prop("disabled", !fileName);
    },

    /**
     * Skip semantic map upload
     */
    skipSemanticMap: function() {
        $("#semanticMapSection").hide();
        this.showStatusMessage("Semantic map upload skipped. You can proceed to describe your data manually.", "info");
        this.enableContinueButton();
    },

    /**
     * Enable the continue button
     */
    enableContinueButton: function() {
        $("#continueButton").prop("disabled", false);
    },

    /**
     * Show a status message
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
     * Handle semantic map upload
     */
    handleSemanticMapUpload: async function() {
        if (!$("#semanticMapFile")[0].files.length) {
            this.showStatusMessage("Please select a semantic map JSON-LD file to upload.", "error");
            return;
        }

        const file = $("#semanticMapFile")[0].files[0];
        const self = this;

        $("#uploadSemanticMapButton").prop("disabled", true).html('<i class="fas fa-spinner fa-spin"></i> Saving...');

        try {
            const fileContents = await this.readFileAsText(file);
            const jsonldData = JSON.parse(fileContents);

            await FlyoverDB.saveData('metadata', {
                key: 'semantic_map',
                data: jsonldData,
                filename: file.name,
                timestamp: new Date().toISOString()
            });

            $("#semanticMapSection").hide();
            this.showStatusMessage("Global semantic map uploaded successfully! This will guide the semantic mapping process and help standardise your data annotations.", "success");
            this.enableContinueButton();
        } catch (error) {
            console.error("Error saving semantic map to IndexedDB:", error);
            let errorMessage = "Failed to save semantic map file.";
            if (error.message) {
                errorMessage += " " + error.message;
            }
            this.showStatusMessage(errorMessage, "error");
            $("#uploadSemanticMapButton").prop("disabled", false).html('<i class="fas fa-upload"></i> Upload Semantic Map');
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
            reader.onerror = (e) => reject(new Error("Failed to read file"));
            reader.readAsText(file);
        });
    },

    /**
     * Start loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    startLoadingAnimation: function(buttonSelector) {
        const button = $(buttonSelector);
        const originalText = button.html();

        button.prop('disabled', true);

        let isMagnifyingArrow = false;
        button.html('<i class="fas fa-search loading-icon"></i> Retrieving variable names...');

        const loadingInterval = setInterval(function() {
            const icon = button.find('.loading-icon');

            icon.addClass('icon-fade-out');

            setTimeout(function() {
                if (isMagnifyingArrow) {
                    icon.removeClass('fa-magnifying-glass-arrow-right').addClass('fa-search');
                } else {
                    icon.removeClass('fa-search').addClass('fa-magnifying-glass-arrow-right');
                }
                isMagnifyingArrow = !isMagnifyingArrow;

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

// Global functions for template event handlers
function updateSemanticMapPath(input) {
    DescribeLandingPage.updateSemanticMapPath(input);
}

function skipSemanticMap() {
    DescribeLandingPage.skipSemanticMap();
}

// Initialize on DOM ready
$(document).ready(function() {
    DescribeLandingPage.init();
});

// Export for global access
window.DescribeLandingPage = DescribeLandingPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeLandingPage;
}

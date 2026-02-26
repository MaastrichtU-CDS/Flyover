/**
 * Index Page Module
 * Handles the landing page workflow card functionality.
 */

const IndexPage = {
    /**
     * Initialize the index page functionality
     */
    init: function() {
        this.checkDataExistence();
        this.bindCardClickHandlers();
    },

    /**
     * Check if data exists and update card states
     */
    checkDataExistence: function() {
        const self = this;
        
        $.ajax({
            url: '/api/check-graph-exists',
            type: 'GET',
            success: function(response) {
                const dataExists = response.exists || false;
                window.graph_exists = dataExists;
                
                self.updateCardStates(dataExists);
                
                // Update navigation header if available
                if (window.flyoverNavigation && window.flyoverNavigation.updateStepStates) {
                    window.flyoverNavigation.dataExists = dataExists;
                    window.flyoverNavigation.updateStepStates();
                }
            },
            error: function() {
                self.updateCardStates(false);
            }
        });
    },

    /**
     * Update workflow card states based on data existence
     * @param {boolean} dataExists - Whether data exists in GraphDB
     */
    updateCardStates: function(dataExists) {
        // Ingest card is always accessible
        $('#ingest-card').removeClass('disabled')
            .removeAttr('data-toggle data-placement title');

        // Describe and Annotate cards require data
        if (!dataExists) {
            $('#describe-card, #annotate-card').addClass('disabled')
                .attr('data-toggle', 'tooltip')
                .attr('data-placement', 'top')
                .attr('title', 'First complete the data ingest step to proceed with this step.');

            // Initialize tooltips for disabled cards
            $('#describe-card, #annotate-card').tooltip();
        } else {
            $('#describe-card, #annotate-card').removeClass('disabled')
                .removeAttr('data-toggle data-placement title')
                .tooltip('dispose');
        }
    },

    /**
     * Bind click handlers for workflow cards
     */
    bindCardClickHandlers: function() {
        $('.workflow-card').click(function(e) {
            const target = $(this).data('target');
            
            if ($(this).hasClass('disabled')) {
                e.preventDefault();

                if (target === '/describe_landing' || target === '/annotation_landing') {
                    alert('Please complete the Ingest step first by submitting your data.');
                }
                return false;
            }

            // Navigate to the target URL
            if (target) {
                window.location.href = target;
            }
        });
    }
};

// Initialize on DOM ready
$(document).ready(function() {
    // Set default state
    window.graph_exists = false;
    
    IndexPage.init();
});

// Export for global access
window.IndexPage = IndexPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = IndexPage;
}

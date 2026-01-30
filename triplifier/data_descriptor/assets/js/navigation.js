/**
 * Flyover Navigation Module
 * Handles navigation header functionality, step tracking, and state management.
 */

const FlyoverNavigation = {
    // Step configuration
    steps: {
        'ingest': {
            pages: ['/ingest', '/upload', '/data-submission'],
            elementId: 'ingest-step',
            alwaysAccessible: true,
            icon: 'fa-cookie-bite',
            completedIcon: 'fa-check'
        },
        'describe': {
            pages: ['/describe_landing', '/describe_variables', '/describe_variable_details', '/describe_downloads'],
            elementId: 'describe-step',
            requiresData: true,
            icon: 'fa-edit',
            completedIcon: 'fa-check'
        },
        'annotate': {
            pages: ['/annotation_landing', '/annotation-review', '/annotation-verify', '/start-annotation'],
            elementId: 'annotate-step',
            requiresData: true,
            icon: 'fa-tags',
            completedIcon: 'fa-check'
        }
    },

    currentStep: null,
    dataExists: false,

    /**
     * Initialize the navigation module
     */
    init: function() {
        this.detectCurrentStep();
        this.checkDataExists();
        this.bindClickHandlers();
        this.updateStepStates();
    },

    /**
     * Detect current step based on page path
     */
    detectCurrentStep: function() {
        const currentPath = window.location.pathname;
        
        for (const [stepName, stepConfig] of Object.entries(this.steps)) {
            if (stepConfig.pages.some(page => currentPath === page || currentPath.startsWith(page))) {
                this.currentStep = stepName;
                break;
            }
        }
    },

    /**
     * Check if data exists in the database
     * @returns {boolean}
     */
    checkDataExists: function() {
        // Check if graph_exists is already defined from server-side template
        if (typeof window.graph_exists !== 'undefined') {
            this.dataExists = window.graph_exists;
            return this.dataExists;
        }

        // Make synchronous AJAX call to check data existence
        const self = this;
        $.ajax({
            url: '/api/check-graph-exists',
            type: 'GET',
            async: false,
            success: function(response) {
                self.dataExists = response.exists || false;
            },
            error: function() {
                self.dataExists = false;
            }
        });

        return this.dataExists;
    },

    /**
     * Check if data exists (async version for when blocking is not needed)
     * @returns {Promise<boolean>}
     */
    checkDataExistsAsync: async function() {
        try {
            const response = await $.ajax({
                url: '/api/check-graph-exists',
                type: 'GET'
            });
            this.dataExists = response.exists || false;
        } catch (error) {
            this.dataExists = false;
        }
        return this.dataExists;
    },

    /**
     * Get step element by step name
     * @param {string} stepName
     * @returns {HTMLElement|null}
     */
    getStepElement: function(stepName) {
        const config = this.steps[stepName];
        return config ? document.getElementById(config.elementId) : null;
    },

    /**
     * Reset all step states
     */
    resetStepStates: function() {
        for (const [stepName, stepConfig] of Object.entries(this.steps)) {
            const element = this.getStepElement(stepName);
            if (!element) continue;

            element.classList.remove('active', 'completed', 'disabled');
            
            // Reset icon to original
            const icon = element.querySelector('.step-icon-header');
            if (icon) {
                icon.className = `fas ${stepConfig.icon} step-icon-header`;
            }
        }
    },

    /**
     * Mark a step as completed
     * @param {string} stepName
     */
    markStepCompleted: function(stepName) {
        const element = this.getStepElement(stepName);
        const config = this.steps[stepName];
        
        if (!element || !config) return;

        element.classList.add('completed');
        
        const icon = element.querySelector('.step-icon-header');
        if (icon) {
            icon.className = `fas ${config.completedIcon} step-icon-header`;
        }
    },

    /**
     * Mark a step as active
     * @param {string} stepName
     */
    markStepActive: function(stepName) {
        const element = this.getStepElement(stepName);
        if (element) {
            element.classList.add('active');
        }
    },

    /**
     * Mark a step as disabled
     * @param {string} stepName
     */
    markStepDisabled: function(stepName) {
        const element = this.getStepElement(stepName);
        if (element) {
            element.classList.add('disabled');
        }
    },

    /**
     * Update all step states based on current context
     */
    updateStepStates: function() {
        this.resetStepStates();

        // Set current step as active
        if (this.currentStep) {
            this.markStepActive(this.currentStep);
        }

        // Mark ingest as completed if data exists
        if (this.dataExists) {
            this.markStepCompleted('ingest');

            // Mark describe as completed if we're on annotate step
            if (this.currentStep === 'annotate') {
                this.markStepCompleted('describe');
            }
        }

        // Handle index page specifically
        const currentPath = window.location.pathname;
        if (currentPath === '/' && this.dataExists) {
            const ingestElement = this.getStepElement('ingest');
            if (ingestElement) {
                ingestElement.classList.remove('active');
            }
            this.markStepCompleted('ingest');
        }

        // Disable steps that require data when no data exists
        if (!this.dataExists) {
            for (const [stepName, stepConfig] of Object.entries(this.steps)) {
                if (stepConfig.requiresData && stepName !== this.currentStep) {
                    this.markStepDisabled(stepName);
                }
            }
        }
    },

    /**
     * Bind click handlers for step navigation
     */
    bindClickHandlers: function() {
        const self = this;

        for (const [stepName, stepConfig] of Object.entries(this.steps)) {
            const element = this.getStepElement(stepName);
            if (!element) continue;

            element.addEventListener('click', function(e) {
                if (this.classList.contains('disabled')) {
                    e.preventDefault();

                    if (stepConfig.requiresData && !self.dataExists) {
                        alert('Please complete the Ingest step first by submitting your data.');
                    }
                    return false;
                }
            });
        }
    },

    /**
     * Check if a specific step is accessible
     * @param {string} stepName
     * @returns {boolean}
     */
    isStepAccessible: function(stepName) {
        const config = this.steps[stepName];
        if (!config) return false;

        if (config.alwaysAccessible) return true;
        if (config.requiresData && !this.dataExists) return false;

        return true;
    },

    /**
     * Navigate to a specific step
     * @param {string} stepName
     */
    navigateToStep: function(stepName) {
        if (!this.isStepAccessible(stepName)) {
            console.warn(`Step ${stepName} is not accessible`);
            return;
        }

        const config = this.steps[stepName];
        if (config && config.pages && config.pages.length > 0) {
            window.location.href = config.pages[0];
        }
    }
};

// Initialize on DOM ready
$(document).ready(function() {
    FlyoverNavigation.init();
});

// Export for global access
window.flyoverNavigation = FlyoverNavigation;
window.FlyoverNavigation = FlyoverNavigation;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FlyoverNavigation;
}

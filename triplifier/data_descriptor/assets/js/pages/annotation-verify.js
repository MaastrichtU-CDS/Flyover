/**
 * Annotation Verify Page Module
 * Handles the annotation verification page functionality.
 */

const AnnotationVerifyPage = {
    itemsPerPage: 10,
    currentPage: 1,
    variables: [],
    variableItems: null,

    /**
     * Initialize the annotation verify page
     * @param {Object} variableData - Variable data from server
     */
    init: function(variableData) {
        this.variables = Object.keys(variableData || {});
        this.variableItems = document.querySelectorAll('.variable-item');
        
        this.bindEventHandlers();
        this.updateDisplay();
        this.processAnnotations();
    },

    /**
     * Bind event handlers
     */
    bindEventHandlers: function() {
        const self = this;

        document.getElementById('searchInput').addEventListener('input', function() {
            self.currentPage = 1;
            self.updateDisplay();
        });

        document.getElementById('statusFilter').addEventListener('change', function() {
            self.currentPage = 1;
            self.updateDisplay();
        });
    },

    /**
     * Get visible items based on current filters
     * @returns {HTMLElement[]} Filtered items
     */
    getVisibleItems: function() {
        const searchTerm = document.getElementById('searchInput').value.toLowerCase();
        const statusFilter = document.getElementById('statusFilter').value;
        
        return Array.from(this.variableItems).filter(item => {
            const name = item.querySelector('.variable-name').textContent.toLowerCase();
            const status = item.dataset.status;
            const matchesSearch = name.includes(searchTerm);
            const matchesStatus = statusFilter === 'all' || status === statusFilter;
            return matchesSearch && matchesStatus;
        });
    },

    /**
     * Update pagination controls
     */
    updatePaginationControls: function() {
        const totalPages = Math.ceil(this.getVisibleItems().length / this.itemsPerPage);
        const pagination = document.getElementById('pagination');

        if (totalPages <= 1) {
            pagination.style.display = 'none';
            return;
        }

        pagination.style.display = 'block';
        document.getElementById('currentPage').textContent = this.currentPage;
        document.getElementById('totalPages').textContent = totalPages;
        document.getElementById('prevBtn').disabled = this.currentPage <= 1;
        document.getElementById('nextBtn').disabled = this.currentPage >= totalPages;
    },

    /**
     * Update the display based on current page and filters
     */
    updateDisplay: function() {
        const visibleItems = this.getVisibleItems();
        const start = (this.currentPage - 1) * this.itemsPerPage;
        const end = start + this.itemsPerPage;

        this.variableItems.forEach(item => item.classList.add('hidden'));
        visibleItems.slice(start, end).forEach(item => item.classList.remove('hidden'));

        this.updatePaginationControls();
    },

    /**
     * Change the current page
     * @param {number} direction - Direction to change page (+1 or -1)
     */
    changePage: function(direction) {
        const totalPages = Math.ceil(this.getVisibleItems().length / this.itemsPerPage);
        const newPage = this.currentPage + direction;
        
        if (newPage >= 1 && newPage <= totalPages) {
            this.currentPage = newPage;
            this.updateDisplay();
        }
    },

    /**
     * Process annotations for all variables
     */
    processAnnotations: function() {
        const self = this;

        this.variables.forEach(function(variableName) {
            const safeId = variableName.replace(/\./g, '-');
            const iconId = 'icon-' + safeId;
            const messageId = 'message-' + safeId;
            const item = document.querySelector(`[id="message-${safeId}"]`).closest('.variable-item');

            fetch('/verify-annotation-ask', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({variable: variableName})
            })
            .then(response => response.json())
            .then(data => {
                const icon = document.getElementById(iconId);
                const message = document.getElementById(messageId);
                
                if (data.success && typeof(data.valid) !== 'undefined') {
                    if (data.valid) {
                        icon.innerHTML = '<i class="fas fa-check-circle success"></i>';
                        message.textContent = 'Successfully annotated';
                        item.dataset.status = 'success';
                    } else {
                        icon.innerHTML = '<i class="fas fa-times-circle error"></i>';
                        message.textContent = 'Annotation failed';
                        item.dataset.status = 'error';
                    }
                } else {
                    icon.innerHTML = '<i class="fas fa-times-circle error"></i>';
                    message.textContent = 'Error: ' + (data.error || 'Unknown error');
                    item.dataset.status = 'error';
                }
                self.updateDisplay();
            })
            .catch(error => {
                const icon = document.getElementById(iconId);
                const message = document.getElementById(messageId);
                icon.innerHTML = '<i class="fas fa-times-circle error"></i>';
                message.textContent = 'Network error';
                item.dataset.status = 'error';
                self.updateDisplay();
            });
        });
    }
};

// Global function for pagination
function changePage(direction) {
    AnnotationVerifyPage.changePage(direction);
}

// Export for global access
window.AnnotationVerifyPage = AnnotationVerifyPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AnnotationVerifyPage;
}

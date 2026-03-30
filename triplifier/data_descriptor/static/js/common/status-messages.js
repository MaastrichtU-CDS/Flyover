/**
 * Status Messages Module
 * Provides reusable status message/alert functionality.
 */

const StatusMessages = {
    /**
     * Show a status message alert
     * @param {string} containerId - ID of the container element (without #)
     * @param {string} message - Message to display
     * @param {string} type - Type: 'success', 'error', 'info', 'warning'
     * @param {boolean} dismissible - Whether the alert is dismissible
     */
    show: function(containerId, message, type, dismissible) {
        type = type || 'success';
        dismissible = dismissible !== false;
        
        const alertClass = {
            'error': 'alert-danger',
            'danger': 'alert-danger',
            'info': 'alert-info',
            'warning': 'alert-warning',
            'success': 'alert-success'
        }[type] || 'alert-success';
        
        const iconClass = {
            'error': 'fa-exclamation-triangle',
            'danger': 'fa-exclamation-triangle',
            'info': 'fa-info-circle',
            'warning': 'fa-exclamation-triangle',
            'success': 'fa-check-circle'
        }[type] || 'fa-check-circle';
        
        let alertHtml = `
            <div class="alert ${alertClass}${dismissible ? ' alert-dismissible fade show' : ''}" role="alert">
                <i class="fas ${iconClass}"></i>
                ${message}`;
        
        if (dismissible) {
            alertHtml += `
                <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>`;
        }
        
        alertHtml += '</div>';
        
        $(`#${containerId}`).html(alertHtml);
    },

    /**
     * Clear all messages from a container
     * @param {string} containerId - ID of the container element (without #)
     */
    clear: function(containerId) {
        $(`#${containerId}`).empty();
    },

    /**
     * Append a message to a container (instead of replacing)
     * @param {string} containerId - ID of the container element (without #)
     * @param {string} message - Message to display
     * @param {string} type - Type: 'success', 'error', 'info', 'warning'
     */
    append: function(containerId, message, type) {
        type = type || 'info';
        
        const alertClass = {
            'error': 'alert-danger',
            'danger': 'alert-danger',
            'info': 'alert-info',
            'warning': 'alert-warning',
            'success': 'alert-success'
        }[type] || 'alert-info';
        
        const iconClass = {
            'error': 'fa-exclamation-triangle',
            'danger': 'fa-exclamation-triangle',
            'info': 'fa-info-circle',
            'warning': 'fa-exclamation-triangle',
            'success': 'fa-check-circle'
        }[type] || 'fa-info-circle';
        
        const alertHtml = `
            <div class="alert ${alertClass}" role="alert" style="font-size: 0.9em;">
                <i class="fas ${iconClass}"></i>
                ${message}
            </div>`;
        
        $(`#${containerId}`).append(alertHtml);
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
    }
};

// Export for global access
window.StatusMessages = StatusMessages;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = StatusMessages;
}

/**
 * Toggle Controls Module
 * Provides reusable collapsible section toggle functionality.
 */

const ToggleControls = {
    /**
     * Bind toggle handlers for expand/collapse buttons
     * Uses standard toggle button structure with .toggle-text and .angle-icon
     */
    bindToggleHandlers: function() {
        $('.toggle-button').off('click').on('click', function() {
            const textElement = $(this).find('.toggle-text');
            const svgElement = $(this).find('.angle-icon path');
            
            if (textElement.text() === 'Show more') {
                textElement.text('Show less');
                // Down arrow SVG path
                svgElement.attr('d', 'M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z');
            } else {
                textElement.text('Show more');
                // Up arrow SVG path
                svgElement.attr('d', 'M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8-12.5-45.3 0s-12.5 32.8 0-45.3l160-160z');
            }
            $(this).toggleClass('open');
            $(this).next('.content').toggleClass('active');
        });
    },

    /**
     * Bind item-level toggle handlers (for nested toggles)
     */
    bindItemToggleHandlers: function() {
        $('.item-toggle-button').off('click').on('click', function() {
            $(this).toggleClass('open');
            $(this).closest('.variable-row').next('.toggle-content').toggleClass('active');
        });
    },

    /**
     * Initialize all toggle handlers
     */
    init: function() {
        this.bindToggleHandlers();
        this.bindItemToggleHandlers();
    }
};

// Export for global access
window.ToggleControls = ToggleControls;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ToggleControls;
}

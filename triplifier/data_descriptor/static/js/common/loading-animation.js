/**
 * Loading Animation Module
 * Provides reusable loading animation functionality for buttons.
 */

const LoadingAnimation = {
    intervals: {},

    /**
     * Start a loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     * @param {string} message - Loading message to display
     * @param {string} iconClass1 - First icon class (e.g., 'fa-edit')
     * @param {string} iconClass2 - Second icon class (e.g., 'fa-pen')
     */
    start: function(buttonSelector, message, iconClass1, iconClass2) {
        const button = $(buttonSelector);
        const originalText = button.html();
        
        button.prop('disabled', true).addClass('processing');
        
        let isFirstIcon = true;
        button.html(`<i class="fas ${iconClass1} loading-icon"></i> ${message}`);
        
        const intervalId = setInterval(function() {
            const icon = button.find('.loading-icon');
            
            icon.addClass('icon-fade-out');
            
            setTimeout(function() {
                if (isFirstIcon) {
                    icon.removeClass(iconClass1).addClass(iconClass2);
                } else {
                    icon.removeClass(iconClass2).addClass(iconClass1);
                }
                isFirstIcon = !isFirstIcon;
                
                icon.removeClass('icon-fade-out').addClass('icon-fade-in');
                
                setTimeout(function() {
                    icon.removeClass('icon-fade-in');
                }, 300);
            }, 150);
        }, 1000);
        
        this.intervals[buttonSelector] = intervalId;
        button.data('original-text', originalText);
    },

    /**
     * Stop a loading animation on a button
     * @param {string} buttonSelector - jQuery selector for the button
     */
    stop: function(buttonSelector) {
        const button = $(buttonSelector);
        
        if (this.intervals[buttonSelector]) {
            clearInterval(this.intervals[buttonSelector]);
            delete this.intervals[buttonSelector];
        }
        
        const originalText = button.data('original-text');
        if (originalText) {
            button.html(originalText);
        }
        button.prop('disabled', false).removeClass('processing');
    },

    /**
     * Start search loading animation (magnifying glass icons)
     * @param {string} buttonSelector - jQuery selector for the button
     * @param {string} message - Loading message
     */
    startSearch: function(buttonSelector, message) {
        this.start(buttonSelector, message || 'Searching...', 'fa-search', 'fa-magnifying-glass-arrow-right');
    },

    /**
     * Start edit loading animation (edit/pen icons)
     * @param {string} buttonSelector - jQuery selector for the button
     * @param {string} message - Loading message
     */
    startEdit: function(buttonSelector, message) {
        this.start(buttonSelector, message || 'Processing...', 'fa-edit', 'fa-pen');
    }
};

// Export for global access
window.LoadingAnimation = LoadingAnimation;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = LoadingAnimation;
}

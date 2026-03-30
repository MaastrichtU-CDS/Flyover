/**
 * File Utilities Module
 * Provides reusable file handling functionality.
 */

const FileUtils = {
    /**
     * Read file as text using Promise
     * @param {File} file - File object to read
     * @returns {Promise<string>} File contents as text
     */
    readAsText: function(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => resolve(e.target.result);
            reader.onerror = (e) => reject(new Error('Failed to read file'));
            reader.readAsText(file);
        });
    },

    /**
     * Update a text input with the selected filename
     * @param {HTMLInputElement} fileInput - The file input element
     * @param {string} displayInputId - ID of the text input to update
     */
    updateFilePath: function(fileInput, displayInputId) {
        const fullPath = fileInput.value;
        const fileName = fullPath.split('\\').pop();
        $(`#${displayInputId}`).val(fileName);
        return fileName;
    },

    /**
     * Download JSON data as a file
     * @param {Object} data - Data to download
     * @param {string} filename - Filename for download
     * @param {string} mimeType - MIME type (default: application/json)
     */
    downloadJson: function(data, filename, mimeType) {
        mimeType = mimeType || 'application/json';
        const jsonString = JSON.stringify(data, null, 2);
        const blob = new Blob([jsonString], { type: mimeType });
        
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    },

    /**
     * Parse JSON file safely
     * @param {string} content - File content to parse
     * @returns {Object|null} Parsed JSON or null if invalid
     */
    parseJsonSafely: function(content) {
        try {
            return JSON.parse(content);
        } catch (e) {
            console.error('Failed to parse JSON:', e);
            return null;
        }
    }
};

// Export for global access
window.FileUtils = FileUtils;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FileUtils;
}

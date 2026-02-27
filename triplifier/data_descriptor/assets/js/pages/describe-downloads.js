/**
 * Describe Downloads Page Module
 * Handles the describe downloads page functionality including semantic map download.
 */

const DescribeDownloadsPage = {
    /**
     * Initialize the describe downloads page
     */
    init: function() {
        this.checkSemanticMapAvailability();
    },

    /**
     * Check if semantic map is available in IndexedDB
     */
    checkSemanticMapAvailability: async function() {
        try {
            if (!FlyoverDB.isSupported()) {
                console.warn('Flyover: IndexedDB not supported');
                return;
            }

            await FlyoverDB.initDB();
            console.log('Flyover: IndexedDB initialized');

            const result = await FlyoverDB.getData('metadata', 'semantic_map');
            
            if (result && result.data) {
                console.log('Flyover: Semantic map found in IndexedDB, showing download section');
                document.getElementById('semantic-map-section').style.display = 'block';
            } else {
                console.log('Flyover: No semantic map found in IndexedDB, hiding download section');
                document.getElementById('semantic-map-section').style.display = 'none';
            }
        } catch (error) {
            console.error('Flyover: Error checking semantic map availability:', error);
            document.getElementById('semantic-map-section').style.display = 'none';
        }
    },

    /**
     * Download semantic map from IndexedDB
     */
    downloadSemanticMap: async function() {
        try {
            if (!FlyoverDB.isSupported()) {
                alert('IndexedDB is not supported in your browser. Cannot download semantic map.');
                return;
            }

            await FlyoverDB.initDB();
            const result = await FlyoverDB.getData('metadata', 'semantic_map');
            
            if (!result || !result.data) {
                alert('No semantic map found in IndexedDB. Please complete the mapping first.');
                return;
            }

            const jsonString = JSON.stringify(result.data, null, 2);
            const blob = new Blob([jsonString], { type: 'application/ld+json' });
            
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'semantic_mapping.jsonld';
            document.body.appendChild(a);
            a.click();
            
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            
            console.log('Flyover: Semantic map downloaded successfully');
        } catch (error) {
            console.error('Flyover: Error downloading semantic map:', error);
            alert('Error downloading semantic map. Please check the console for details.');
        }
    }
};

// Global function for template event handlers
function downloadSemanticMapFromIndexedDB() {
    DescribeDownloadsPage.downloadSemanticMap();
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    DescribeDownloadsPage.init();
});

// Export for global access
window.DescribeDownloadsPage = DescribeDownloadsPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DescribeDownloadsPage;
}

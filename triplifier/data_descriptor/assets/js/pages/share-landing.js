/**
 * Share Landing Page Module
 * Handles the share landing page functionality including semantic map and ontology downloads.
 */

const ShareLandingPage = {
    /**
     * Initialize the share landing page
     */
    init: function() {
        console.log('Flyover: Initializing Share Landing page');
        this.checkSemanticMapAvailability();
        this.checkOntologyAvailability();
        this.setupEventHandlers();
        console.log('Flyover: Share Landing page initialization complete');
    },

    /**
     * Set up event handlers for the page
     */
    setupEventHandlers: function() {
        // Set up download button click handler
        const downloadBtn = document.getElementById('download-semantic-map-btn');
        if (downloadBtn) {
            downloadBtn.addEventListener('click', function() {
                ShareLandingPage.downloadSemanticMap();
            });
        }
    },

    /**
     * Check if semantic map is available in IndexedDB
     */
    checkSemanticMapAvailability: async function() {
        try {
            console.log('Flyover: Checking semantic map availability in IndexedDB');
            
            // Check if FlyoverDB is available
            if (typeof FlyoverDB === 'undefined') {
                console.warn('Flyover: FlyoverDB not available');
                document.getElementById('semanticMapSection').style.display = 'none';
                return;
            }

            await FlyoverDB.initDB();
            const result = await FlyoverDB.getData('metadata', 'semantic_map');

            if (result && result.data) {
                console.log('Flyover: Valid semantic map found in IndexedDB, showing download section');
                document.getElementById('semanticMapSection').style.display = 'block';
            } else {
                console.log('Flyover: No valid semantic map found in IndexedDB, hiding download section');
                document.getElementById('semanticMapSection').style.display = 'none';
            }
        } catch (error) {
            console.error('Flyover: Error checking semantic map availability:', error);
            console.error('Flyover: This might be expected if no semantic map has been created yet');
            document.getElementById('semanticMapSection').style.display = 'none';
        }
    },

    /**
     * Check if ontology is available in GraphDB
     */
    checkOntologyAvailability: async function() {
        try {
            console.log('Flyover: Checking ontology availability...');
            
            // Directly call the API instead of going through SharedChecks
            // to have better error handling
            const response = await fetch('/api/graphdb-databases');
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();

            if (data.success && data.databases && data.databases.length > 0) {
                console.log('Flyover: Graphs exist, showing ontology download section');
                document.getElementById('ontologySection').style.display = 'block';
            } else {
                console.log('Flyover: No graphs exist, hiding ontology download section');
                document.getElementById('ontologySection').style.display = 'none';
            }
        } catch (error) {
            console.error('Flyover: Error checking ontology availability:', error);
            console.error('Flyover: This might be expected if no data has been uploaded yet');
            document.getElementById('ontologySection').style.display = 'none';
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
    ShareLandingPage.downloadSemanticMap();
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('Flyover: DOM loaded, initializing Share Landing page');
    ShareLandingPage.init();
});

// Also initialize if DOM is already loaded
if (document.readyState === 'complete' || document.readyState === 'interactive') {
    console.log('Flyover: DOM already ready, initializing Share Landing page');
    setTimeout(() => {
        ShareLandingPage.init();
    }, 100);
}

// Export for global access
window.ShareLandingPage = ShareLandingPage;

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ShareLandingPage;
}
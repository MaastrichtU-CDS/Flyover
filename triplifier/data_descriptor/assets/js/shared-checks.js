/**
 * Shared Utility Functions for Flyover Pages
 * Common checks for semantic maps and graph databases
 */

const SharedChecks = {
    // Store GraphDB databases globally
    graphDbDatabases: [],

    /**
     * Check if IndexedDB contains a valid semantic map
     * @returns {Promise<{hasMap: boolean, mapData: object|null, hasDatabases: boolean, hasVariableInfo: boolean}>}
     */
    async checkSemanticMapInIndexedDB() {
        try {
            if (typeof FlyoverDB === 'undefined') {
                console.warn('FlyoverDB not available');
                return { hasMap: false, mapData: null, hasDatabases: false, hasVariableInfo: false };
            }

            await FlyoverDB.initDB();
            const result = await FlyoverDB.getData('metadata', 'semantic_map');

            if (result && result.data) {
                console.log('IndexedDB: Found semantic map');
                
                // Check if the map has a databases section
                const hasDatabases = result.data.databases && 
                                     Object.keys(result.data.databases).length > 0;
                
                // Check if the map has a variable_info section
                const hasVariableInfo = result.data.variable_info && 
                                         Object.keys(result.data.variable_info).length > 0;
                
                return {
                    hasMap: true,
                    mapData: result.data,
                    hasDatabases: hasDatabases,
                    hasVariableInfo: hasVariableInfo
                };
            } else {
                console.log('IndexedDB: No semantic map found');
                return { hasMap: false, mapData: null, hasDatabases: false, hasVariableInfo: false };
            }
        } catch (error) {
            console.error('Error checking semantic map in IndexedDB:', error);
            return { hasMap: false, mapData: null, hasDatabases: false, hasVariableInfo: false };
        }
    },

    /**
     * Check if graphs exist in GraphDB
     * @returns {Promise<{hasGraphs: boolean, databases: Array}>}
     */
    async checkGraphsInGraphDB() {
        try {
            const response = await fetch('/api/graphdb-databases');
            const data = await response.json();

            if (data.success && data.databases && data.databases.length > 0) {
                console.log('GraphDB: Found databases:', data.databases);
                this.graphDbDatabases = data.databases;
                return { hasGraphs: true, databases: data.databases };
            } else {
                console.warn('GraphDB: No databases found:', data.message);
                return { hasGraphs: false, databases: [] };
            }
        } catch (error) {
            console.error('Error fetching GraphDB databases:', error);
            return { hasGraphs: false, databases: [] };
        }
    },

    /**
     * Extract table names from JSON-LD structure
     * @param {object} data - JSON-LD data
     * @returns {Array} - Array of table names
     */
    extractJsonLdTables(data) {
        const tables = [];
        if (data.databases) {
            for (const [dbKey, dbData] of Object.entries(data.databases)) {
                if (dbData.tables) {
                    for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
                        // Use sourceFile if available, otherwise use the table key
                        const tableName = (typeof tableData === 'object' && tableData.sourceFile)
                            ? tableData.sourceFile
                            : tableKey;
                        tables.push(tableName);
                    }
                }
            }
        } else if (data.database_name) {
            // Fallback for flat structure
            tables.push(data.database_name);
        }
        return tables;
    },

    /**
     * Find matching database between map and GraphDB
     * @param {string} mapDbName - Database name from map
     * @param {Array} graphDbList - List of GraphDB databases
     * @returns {string|null} - Matching database or null
     */
    findMatchingDatabase(mapDbName, graphDbList) {
        if (!mapDbName || mapDbName === '') return null;

        for (const db of graphDbList) {
            if (db === mapDbName) return db;

            // Try matching without .csv extension
            const mapNoExt = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName;
            const dbNoExt = db.endsWith('.csv') ? db.slice(0, -4) : db;
            if (mapNoExt === dbNoExt) return db;
        }
        return null;
    },

    /**
     * Generate database comparison HTML
     * @param {Array} jsonldTables - Tables from JSON-LD
     * @param {Array} graphDbList - Databases from GraphDB
     * @returns {object} - Comparison result with HTML and stats
     */
    generateDatabaseComparisonHtml(jsonldTables, graphDbList) {
        const matching = [];
        const nonMatchingJsonld = [];
        const nonMatchingGraphDb = [...graphDbList];

        for (const jsonldTable of jsonldTables) {
            const match = this.findMatchingDatabase(jsonldTable, graphDbList);
            if (match) {
                matching.push({ jsonld: jsonldTable, graphdb: match });
                const idx = nonMatchingGraphDb.indexOf(match);
                if (idx > -1) nonMatchingGraphDb.splice(idx, 1);
            } else {
                nonMatchingJsonld.push(jsonldTable);
            }
        }

        let html = '';

        if (matching.length > 0) {
            html += `<div class="text-success mb-2" style="font-size: 0.9em;">
                <i class="fas fa-check-circle"></i> <strong>${matching.length}</strong> data source(s) ready
            </div>`;
        }

        if (nonMatchingJsonld.length > 0) {
            html += `<div class="text-warning mb-2" style="font-size: 0.9em;">
                <i class="fas fa-exclamation-triangle"></i> <strong>Not in GraphDB:</strong> ${nonMatchingJsonld.map(t => this.escapeHtml(t)).join(', ')}
            </div>`;
        }

        if (nonMatchingGraphDb.length > 0) {
            html += `<div class="text-muted mb-2" style="font-size: 0.9em;">
                <i class="fas fa-info-circle"></i> <strong>Other data in GraphDB:</strong> ${nonMatchingGraphDb.map(t => this.escapeHtml(t)).join(', ')}
            </div>`;
        }

        return {
            html: html,
            hasMatches: matching.length > 0,
            matching: matching,
            nonMatchingJsonld: nonMatchingJsonld,
            nonMatchingGraphDb: nonMatchingGraphDb
        };
    },

    /**
     * Escape HTML to prevent XSS
     * @param {string} text - Text to escape
     * @returns {string} - Escaped text
     */
    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    },

    /**
     * Get statistics about the semantic map
     * @param {object} mapData - JSON-LD data
     * @returns {object} - Statistics object
     */
    getSemanticMapStats(mapData) {
        if (!mapData) return { dbCount: 0, tableCount: 0, columnCount: 0, varCount: 0 };

        const databases = mapData.databases || {};
        const variable_info = mapData.variable_info || {};
        
        let dbCount = Object.keys(databases).length;
        let varCount = Object.keys(variable_info).length;
        let tableCount = 0;
        let columnCount = 0;

        Object.values(databases).forEach(db => {
            const tables = db.tables || {};
            tableCount += Object.keys(tables).length;

            // Count columns across all tables
            Object.values(tables).forEach(table => {
                const columns = table.columns || {};
                columnCount += Object.keys(columns).length;
            });
        });

        return { dbCount, tableCount, columnCount, varCount };
    }
};

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SharedChecks;
}